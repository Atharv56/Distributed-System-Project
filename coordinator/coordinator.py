import asyncio, json, os, time, grpc, random
from typing import List, Tuple
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from utils.config import NodeAddr, ClusterConfig
from utils.snapshot import save_snapshot
from rpc import primalitytest_pb2 as pb
from rpc import primalitytest_pb2_grpc as rpc

# --- Simple config in-code for quick start ---
COORD = NodeAddr("127.0.0.1", 7000)
WORKERS = [NodeAddr("127.0.0.1", 7100+i) for i in range(3)]
CFG = ClusterConfig(coordinator=COORD, workers=WORKERS)

# ---- Coordinator gRPC server implements ControlService & CoordinatorService
class ControlService(rpc.ControlServiceServicer):
    def __init__(self, node):
        self.node = node

    async def Heartbeat(self, request, context):
        return pb.Ack(ok=True, msg="alive")

    async def SnapshotMarker(self, req, ctx):
        # First marker at coordinator might be from itself; ignore content
        return pb.Ack(ok=True)

    async def SnapshotCollect(self, req, ctx):
        state = self.node.capture_local_state()
        path = save_snapshot(req.snapshot_id, "coord", state)
        return pb.SnapshotBlob(snapshot_id=req.snapshot_id, node_id="coord",
                               json_state=json.dumps(state))

    async def Restore(self, req, ctx):
        # Coordinator has only dispatch queue in this MVP; nothing to do
        return pb.Ack(ok=True)

class CoordinatorService(rpc.CoordinatorServiceServicer):
    def __init__(self, node):
        self.node = node

    async def AssignChunk(self, chunk, ctx):
        # Workers call this only for sanity; real direction is from coordinator to worker
        return pb.Ack(ok=True, msg="use outbound AssignChunk from coordinator")

class CoordinatorNode:
    def __init__(self, cfg: ClusterConfig, input_files: List[str], chunk_bytes=200_000):
        self.cfg = cfg
        self.input_files = input_files
        self.chunk_bytes = chunk_bytes
        self.work_queue = self._build_queue()

    def _build_queue(self) -> List[Tuple[str,int,int]]:
        q = []
        for fp in self.input_files:
            size = os.path.getsize(fp)
            off = 0
            while off < size:
                end = min(off + self.chunk_bytes, size)
                q.append((fp, off, end))
                off = end
        random.shuffle(q)
        return q

    def capture_local_state(self):
        return {"pending_chunks": len(self.work_queue), "ts": time.time()}

    async def _assign(self, worker_addr: NodeAddr, chunk):
        async with grpc.aio.insecure_channel(f"{worker_addr.host}:{worker_addr.port}") as ch:
            stub = rpc.CoordinatorServiceStub(ch)
            await stub.AssignChunk(pb.Chunk(file=chunk[0], start=chunk[1], end=chunk[2]))

    async def dispatch_loop(self):
        i = 0
        while self.work_queue:
            chunk = self.work_queue.pop()
            target = self.cfg.workers[i % len(self.cfg.workers)]
            i += 1
            await self._assign(target, chunk)
        print("[coord] dispatched all chunks")

    async def take_snapshot(self, snapshot_id: str):
        # 1) send marker to all nodes (including workers)
        async def send_marker(addr: NodeAddr):
            async with grpc.aio.insecure_channel(f"{addr.host}:{addr.port}") as ch:
                stub = rpc.ControlServiceStub(ch)
                await stub.SnapshotMarker(pb.SnapshotMarkerReq(
                    snapshot_id=snapshot_id, from_node="coord"
                ))
        await asyncio.gather(*(send_marker(w) for w in self.cfg.workers))

        # 2) collect snapshots
        async def collect(addr: NodeAddr):
            async with grpc.aio.insecure_channel(f"{addr.host}:{addr.port}") as ch:
                stub = rpc.ControlServiceStub(ch)
                return await stub.SnapshotCollect(pb.SnapshotCollectReq(
                    snapshot_id=snapshot_id
                ))
        results = await asyncio.gather(*(collect(w) for w in self.cfg.workers))
        # include coordinator’s own
        save_snapshot(snapshot_id, "coord", self.capture_local_state())
        with open(os.path.join("snapshots", f"index_{snapshot_id}.json"), "w") as f:
            json.dump([json.loads(r.json_state) for r in results], f, indent=2)

async def main():
    node = CoordinatorNode(CFG, input_files=["numbers.txt"])
    # start gRPC server
    server = grpc.aio.server()
    rpc.add_ControlServiceServicer_to_server(ControlService(node), server)
    rpc.add_CoordinatorServiceServicer_to_server(CoordinatorService(node), server)
    server.add_insecure_port(f"{COORD.host}:{COORD.port}")
    await server.start()
    print(f"[coord] listening on {COORD.host}:{COORD.port}")

    # schedule dispatch and periodic snapshots
    asyncio.create_task(node.dispatch_loop())

    async def snapshotter():
        while True:
            sid = f"snap-{int(time.time())}"
            print(f"[coord] snapshot {sid}")
            await node.take_snapshot(sid)
            await asyncio.sleep(20)
    asyncio.create_task(snapshotter())

    await server.wait_for_termination()

if __name__ == "__main__":
    # Create a small demo input file if missing
    if not os.path.exists("numbers.txt"):
        with open("numbers.txt","w") as f:
            for i in range(2, 500_000):
                f.write(str(i)+"\n")
    asyncio.run(main())

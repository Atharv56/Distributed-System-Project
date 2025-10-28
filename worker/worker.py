import asyncio, os, time, json, hashlib, random, grpc
from typing import Dict, Set, Tuple, List

# --- Path setup so imports work even when run directly ---
import sys, pathlib
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

# --- Local imports ---
from utils.primes import is_prime_64
from utils.snapshot import save_snapshot
from utils.config import NodeAddr
from rpc import primalitytest_pb2 as pb
from rpc import primalitytest_pb2_grpc as rpc


# ============================ CONFIG ============================= #
WORKER_ID = os.getenv("WORKER_ID", "w0")
HOST = os.getenv("HOST", "127.0.0.1")
PORT = int(os.getenv("PORT", "7100"))
COORD_HOST = os.getenv("COORD_HOST", "127.0.0.1")
COORD_PORT = int(os.getenv("COORD_PORT", "7000"))

SHARD_COUNT = int(os.getenv("SHARD_COUNT", "2"))
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))

DB_DIR = os.getenv("DB_DIR", f"sharddb_{WORKER_ID}")
os.makedirs(DB_DIR, exist_ok=True)

PRIMES_PATH = os.path.join(DB_DIR, "primes.txt")
MSGS_PATH = os.path.join(DB_DIR, "msgs.txt")

assignments: List[Dict] = []  # work assignments
primes: Set[int] = set()      # local dedup by prime
seen_msg_ids: Set[str] = set()  # message-level dedup


# ============================ STATE HELPERS ============================= #
def _load_state():
    """Load previous primes and message IDs from disk for fault tolerance."""
    if os.path.exists(PRIMES_PATH):
        with open(PRIMES_PATH) as f:
            for line in f:
                try:
                    primes.add(int(line.strip()))
                except:
                    pass
    if os.path.exists(MSGS_PATH):
        with open(MSGS_PATH) as f:
            for line in f:
                seen_msg_ids.add(line.strip())


def _append(path: str, line: str):
    """Append a single line to a file (simple persistence)."""
    with open(path, "a") as f:
        f.write(line + "\n")


_load_state()


# ============================ HASH & SHARDING ============================= #
def _hash_index(n: int) -> int:
    """Return consistent shard index based on MD5 hash of number (balanced)."""
    h = int(hashlib.md5(str(n).encode()).hexdigest(), 16)
    return h % SHARD_COUNT


def _owns(n: int) -> bool:
    """Check if this worker owns the shard responsible for number n."""
    return _hash_index(n) == SHARD_INDEX


def _msg_id(n: int, offset: int) -> str:
    """Generate a unique message ID per (worker, number, file position)."""
    raw = f"{WORKER_ID}:{n}:{offset}:{random.random()}"
    return hashlib.sha256(raw.encode()).hexdigest()


def _local_state():
    """Return serializable snapshot of worker state."""
    return {
        "worker_id": WORKER_ID,
        "shard_index": SHARD_INDEX,
        "count_primes": len(primes),
        "count_msgs": len(seen_msg_ids),
        "assignments": assignments,
        "db": {"primes_path": PRIMES_PATH, "msgs_path": MSGS_PATH},
        "ts": time.time(),
    }


def _capture_and_save(snapshot_id: str):
    save_snapshot(snapshot_id, WORKER_ID, _local_state())


# ============================ gRPC SERVICES ============================= #
class ControlService(rpc.ControlServiceServicer):
    async def Heartbeat(self, req, ctx):
        print(f"💓 Heartbeat from coordinator at {time.strftime('%H:%M:%S')}")
        return pb.Ack(ok=True)

    async def SnapshotMarker(self, req, ctx):
        _capture_and_save(req.snapshot_id)
        print(f"📸 Snapshot marker received: {req.snapshot_id}")
        return pb.Ack(ok=True)

    async def SnapshotCollect(self, req, ctx):
        state = _local_state()
        save_snapshot(req.snapshot_id, WORKER_ID, state)
        return pb.SnapshotBlob(
            snapshot_id=req.snapshot_id, node_id=WORKER_ID, json_state=json.dumps(state)
        )

    async def Restore(self, req, ctx):
        primes.clear()
        seen_msg_ids.clear()
        _load_state()
        print(f"♻️ Restored from snapshot {req.snapshot_id}")
        return pb.Ack(ok=True)


class CoordinatorService(rpc.CoordinatorServiceServicer):
    async def AssignChunk(self, chunk, ctx):
        """Coordinator sends us a chunk to process."""
        a = {"file": chunk.file, "start": chunk.start, "end": chunk.end, "pos": chunk.start}
        assignments.append(a)
        print(f"🧩 Received chunk {chunk.file} ({chunk.start}-{chunk.end})")
        asyncio.create_task(process_assignment(a))
        return pb.Ack(ok=True)


class ShardService(rpc.ShardServiceServicer):
    async def EmitPrime(self, msg, ctx):
        """
        Receive a prime emitted from another worker.
        Implements *global deduplication*: each shard writes a prime only once.
        """
        p = msg.prime

        # --- GLOBAL DEDUP: prime-level check first ---
        if p in primes:
            return pb.Ack(ok=True, msg="duplicate-prime")

        # --- Idempotent message dedup (for network retries) ---
        if msg.msg_id in seen_msg_ids:
            return pb.Ack(ok=True, msg="duplicate-msg")

        seen_msg_ids.add(msg.msg_id)
        _append(MSGS_PATH, msg.msg_id)

        # --- Store the new, unique prime ---
        primes.add(p)
        _append(PRIMES_PATH, str(p))
        print(f"💾 Stored unique prime {p} in shard {SHARD_INDEX}")
        return pb.Ack(ok=True)


# ============================ WORKER LOGIC ============================= #
async def process_assignment(a: Dict):
    """Processes a chunk assigned by coordinator."""
    file_path, pos, end = a["file"], a["pos"], a["end"]
    print(f"⚙️  Processing file chunk {file_path} [{pos}-{end}]")

    with open(file_path) as f:
        f.seek(pos)
        while f.tell() < end:
            line = f.readline()
            if not line:
                break
            try:
                n = int(line.strip())
            except:
                continue

            if is_prime_64(n):
                shard_index = _hash_index(n)
                owner_port = 7100 + shard_index
                msg_id = _msg_id(n, f.tell())
                async with grpc.aio.insecure_channel(f"{HOST}:{owner_port}") as ch:
                    stub = rpc.ShardServiceStub(ch)
                    await stub.EmitPrime(
                        pb.PrimeMsg(worker_id=WORKER_ID, msg_id=msg_id, prime=n)
                    )

            a["pos"] = f.tell()


# ============================ MAIN ============================= #
async def main():
    print(f"🚀 Worker {WORKER_ID} starting...")
    print(f"🔗 Coordinator: {COORD_HOST}:{COORD_PORT}")
    print(f"📡 Listening on {HOST}:{PORT}, shard index {SHARD_INDEX}/{SHARD_COUNT}")

    server = grpc.aio.server()
    rpc.add_ControlServiceServicer_to_server(ControlService(), server)
    rpc.add_CoordinatorServiceServicer_to_server(CoordinatorService(), server)
    rpc.add_ShardServiceServicer_to_server(ShardService(), server)
    server.add_insecure_port(f"{HOST}:{PORT}")

    await server.start()
    print(f"✅ Worker {WORKER_ID} is now running on {HOST}:{PORT}")
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(main())

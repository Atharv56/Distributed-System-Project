import json, os, time
from typing import Dict, Any

def save_snapshot(snapshot_id: str, node_id: str, state: Dict[str, Any], out_dir="snapshots"):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{node_id}_{snapshot_id}.json")
    blob = {"snapshot_id": snapshot_id, "node_id": node_id, "ts": time.time(), "state": state}
    with open(path, "w") as f:
        json.dump(blob, f, indent=2)
    return path

def load_latest_snapshot_id(out_dir="snapshots"):
    if not os.path.exists(out_dir): return None
    files = [x for x in os.listdir(out_dir) if x.endswith(".json")]
    if not files: return None
    # pick latest by mtime
    files.sort(key=lambda p: os.path.getmtime(os.path.join(out_dir, p)), reverse=True)
    with open(os.path.join(out_dir, files[0])) as f:
        return json.load(f)["snapshot_id"]

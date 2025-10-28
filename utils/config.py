from dataclasses import dataclass
from typing import List

@dataclass
class NodeAddr:
    host: str
    port: int

@dataclass
class ClusterConfig:
    coordinator: NodeAddr
    workers: List[NodeAddr]
    # workers also host ShardService; shard_index == list index

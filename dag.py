from pipeline_block import PipelineBlock

from typing import List, Tuple, Dict, Optional
from collections import defaultdict
import graphlib
import concurrent.futures
import time


class DAGNode:

    def __init__(self,
                 id: str,
                 pipeline_block: PipelineBlock,
                 parent_ids: List[str] = []) -> None:
        self.id = id
        self.pipeline_block = pipeline_block
        self.parent_ids = parent_ids

    def run(self) -> bool:
        res = self.pipeline_block.run()
        return res

    def __repr__(self) -> str:
        return f"DAGNode(id={self.id}, pipeline_block={self.pipeline_block}, children={self.child_ids})"


class DAG:

    def __init__(self, nodes: List[DAGNode], num_workers: int = 10) -> None:
        self.nodes = nodes
        self.num_workers = num_workers
        assert self.num_workers > 0
        self.node_id_to_node_map: Dict[str, DAGNode] = {
            node.id: node
            for node in nodes
        }
        self.node_id_to_parent_ids_map: Dict[str, List[str]] = {
            node.id: node.parent_ids
            for node in nodes
        }

        self.node_id_to_node_state_map: Dict[str, Optional[bool]] = {
            k: None
            for k in self.node_id_to_node_map.keys()
        }

        self._validate_graph_edges()

        print("Full dependency graph:")
        print(self.node_id_to_parent_ids_map)

        self.topo_sorter = graphlib.TopologicalSorter(
            self.node_id_to_parent_ids_map)
        self.topo_sorter.prepare()

    def async_run(self, node_id: str, verbose: bool = False) -> bool:
        if verbose:
            print(f"Async running node {node_id}")
        parent_ids = self.node_id_to_parent_ids_map[node_id]
        for parent_id in parent_ids:
            parent_state = self.node_id_to_node_state_map[parent_id]
            if parent_state is None:
                raise ValueError(
                    f"Parent node {parent_id} of {node_id} is not yet run")
            if not parent_state:
                return False
        res = self.node_id_to_node_map[node_id].run()
        if verbose:
            print(f"Async ran node {node_id} with result {res}")
        self.node_id_to_node_state_map[node_id] = res
        self.topo_sorter.done(node_id)
        return res

    def _validate_graph_edges(self) -> None:
        for node in self.nodes:
            for parent_id in node.parent_ids:
                if parent_id not in self.node_id_to_node_map:
                    raise ValueError(
                        f"Unknown parent node {parent_id} possessed by {node}")

    def run(self) -> Tuple[bool, Optional[str]]:
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.num_workers) as executor:
            while self.topo_sorter.is_active():
                print("Poll")
                for node_id, state in self.node_id_to_node_state_map.items():
                    if state is False:
                        return False, node_id

                node_ids = self.topo_sorter.get_ready()
                for node_id in node_ids:
                    executor.submit(self.async_run, node_id)
                time.sleep(0.5)

        return all(self.node_id_to_node_state_map.values()), None

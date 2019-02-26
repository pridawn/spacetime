from multiprocessing import Process, RLock,Manager
from threading import Thread
import json


class Node(object):
    def __eq__(self, other):
        return other.current == self.current

    def __hash__(self):
        return hash(self.current)

    def __init__(self, current, is_master):
        self.current = current
        self.prev_master = None
        self.next_master = None
        self.all_prev = set()
        self.all_next = set()
        self.is_master = is_master

    def set_next(self, version):
        if self.next_master is None:
            self.next_master = version
        self.all_next.add(version)

    def set_prev(self, version):
        if self.prev_master is None:
            self.prev_master = version
        self.all_prev.add(version)


class Edge(object):
    def __init__(self, from_node, to_node, payload):
        self.from_node = from_node
        self.to_node = to_node
        self.payload = payload


class Graph(object):
    def __init__(self):
        self.tail = Node("ROOT", True)
        self.head = self.tail
        self.nodes = {"ROOT": self.tail}
        self.edges = dict()

    def __getitem__(self, key):
        if isinstance(key, slice):
            step_reverse = ((key.step is not None) and (key.step < 0))
            if key.start:
                try:
                    start = self.nodes[key.start]
                except KeyError:
                    return list()
            elif step_reverse:
                start = self.head
            else:
                start = self.tail

            if key.stop:
                end = self.nodes[key.stop]
            elif step_reverse:
                end = self.tail
            else:
                end = self.head
            current = start
            while current != end:
                if step_reverse:
                    edge = self.edges[(current.prev_master, current.current)]
                    current = self.nodes[current.prev_master]
                else:
                    edge = self.edges[(current.current, current.next_master)]
                    current = self.nodes[current.next_master]
                yield edge.payload
            return
        else:
            raise RuntimeError("Cannot deal with non slice operators.")

    def continue_chain(self, from_version, to_version, package):
        from_version_node = self.nodes.setdefault(
            from_version, Node(from_version, from_version == self.head.current))

        to_version_node = self.nodes.setdefault(
            to_version, Node(to_version, from_version == self.head.current))
        to_version_node.set_prev(from_version)

        edge = Edge(from_version, to_version, package)
        self.edges[(from_version, to_version)] = edge

        from_version_node.set_next(to_version)
        if from_version == self.head.current:
            self.head = to_version_node

    def delete_item(self, version):
        pass

    def maintain_edges(self):
        edges_to_del = set()
        for edge in self.edges:
            start, end = self.nodes[edge[0]], self.nodes[edge[1]]
            if not start.is_master and not end.is_master:
                # Can delete the edge that took start to the master line.
                # The other application clearly ignored it.
                # Lets delete so that nodes can be merged.
                for node_v in start.all_next:
                    if self.nodes[node_v].is_master and (start.current, node_v) in self.edges:
                        edges_to_del.add((start.current, node_v))
                        start.next_master = end.current
        for start, end in edges_to_del:
            del self.edges[(start, end)]
            self.nodes[start].all_next.remove(end)
            self.nodes[end].all_prev.remove(start)

    def merge_node(self, node, merger_function):
        del self.nodes[node.current]
        old_change = self.edges[(node.prev_master, node.current)].payload
        new_change = self.edges[(node.current, node.next_master)].payload
        new_payload = merger_function(old_change, new_change)

        del self.edges[(node.prev_master, node.current)]
        del self.edges[(node.current, node.next_master)]
        self.nodes[node.prev_master].all_next.remove(node.current)
        self.nodes[node.next_master].all_prev.remove(node.current)
        if (node.prev_master, node.next_master) not in self.edges:
            self.edges[(node.prev_master, node.next_master)] = Edge(
                node.prev_master, node.next_master, new_payload)
        else:
            # Figure out how to avoid this computation.
            assert self.edges[(node.prev_master, node.next_master)].payload == new_payload, (
                self.edges[(node.prev_master, node.next_master)].payload, new_payload)
        if self.nodes[node.prev_master].next_master == node.current:
            self.nodes[node.prev_master].next_master = node.next_master
        self.nodes[node.prev_master].all_next.add(node.next_master)
        if self.nodes[node.next_master].prev_master == node.current:
            self.nodes[node.next_master].prev_master = node.prev_master
        self.nodes[node.next_master].all_prev.add(node.prev_master)

    def maintain_nodes(self, state_to_ref, merger_function, master):
        mark_for_merge = set()
        mark_for_delete = set()
        for version, node in self.nodes.items():
            if node == self.head or node == self.tail:
                continue
            if version in state_to_ref and state_to_ref[version]:
                # Node is still marked.
                continue
            if len(node.all_next) > 1 or len(node.all_prev) > 1:
                # Node is marked in a branch.
                continue
            if node.is_master == master:
                mark_for_merge.add(node)

        for node in mark_for_merge:
            self.merge_node(node, merger_function)

    def maintain(self, state_to_ref, merger_function):
        # Delete edges that are useless.
        self.maintain_edges()
        # Merge nodes that are chaining without anyone looking at them
        # First divergent.
        self.maintain_nodes(state_to_ref, merger_function, False)
        # The master line.
        self.maintain_nodes(state_to_ref, merger_function, True)


class VersionGraphProcess(Process):

    def __init__(self):
        self.graph = Graph()
        super().__init__()
        self.manager=Manager()
        self.read_write_queue = self.manager.Queue()
        self.daemon = True
        self.start()

    def process_continue_chain(self, from_version, to_version, package):
        self.graph.continue_chain(from_version, to_version, package)

    def process_maintain(self, state_to_ref, merger_function):
        self.graph.maintain(state_to_ref, merger_function)

    def process_get_item(self, key, result_queue):
        result_queue.put(list(self.graph.__getitem__(key)))

    def process_display_graph(self, display_queue):
        display_queue.put(json.dumps([self.graph.head.current:self.graph.tail.current]))

    def run(self):
        while True:
                req = self.read_write_queue.get()
                if req[0] == "continue_chain":
                    from_version, to_version, package = req[1:]
                    self.process_continue_chain(from_version, to_version, package)
                elif req[0] == "maintain":
                    state_to_ref, merger_function = req[1:]
                    self.process_maintain(state_to_ref, merger_function)
                elif req[0] == "get_item":
                    key, result_queue = req[1],req[2]
                    self.process_get_item(key, result_queue)
                elif req[0] == "display":
                    display_queue = req[1]
                    self.process_display_graph(display_queue)

    def continue_chain(self, from_version, to_version, package):
        self.read_write_queue.put(("continue_chain", from_version, to_version, package))

    def maintain(self, state_to_ref, merger_function):
        self.read_write_queue.put(("maintain", state_to_ref, merger_function))

    def __getitem__(self, key):
        result_queue = Manager().Queue()
        self.read_write_queue.put(("get_item", key,result_queue))
        return result_queue.get()

    def display_graph(self):
        display_queue = Manager().Queue()
        self.read_write_queue.put(("display",display_queue))
        return display_queue.get()


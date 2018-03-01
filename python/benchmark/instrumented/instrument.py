import time
import types
import os
import ujson as json
from collections import namedtuple

import psutil

InstrumentedRecord = namedtuple(
    "InstrumentedRecord", ["timestamp", "timedelta", "memory_delta"])

class InstrumentNode(dict):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.values = list()
        super(InstrumentNode, self).__init__(*args, **kwargs)


class instrument(object):
    root = InstrumentNode("root")

    def __init__(self, path):
        if isinstance(path, types.FunctionType):
            raise TypeError("Need to supply path to instrument, not function")
        if not path:
            raise TypeError("instrument path cannot be empty. Found: %s" % path)
        self.node = instrument.root
        for part in path.split("."):
            if part:
                self.node = self.node.setdefault(part, InstrumentNode(part))

    def __call__(self, func):
        if not (isinstance(func, types.FunctionType)
                or hasattr(func, "__call__")):
            raise TypeError(
                "Instrument can only be used on callable objects or functions")

        def wrapped(*args, **kwargs):
            process = psutil.Process(os.getpid())
            start_mem = process.memory_info().rss
            start = time.time()
            return_value = func(*args, **kwargs)
            end = time.time()
            end_mem = process.memory_info().rss
            self.node.values.append(
                InstrumentedRecord(end, end - start, end_mem - start_mem))
            return return_value

        return wrapped

    @classmethod
    def write_and_reset(cls, filename):
        instrument.write(cls.root, filename)
        instrument.reset(cls.root)

    @staticmethod
    def write(node, filename):
        foldername = os.path.dirname(filename)
        if not os.path.exists(foldername):
            os.makedirs(foldername)
        # pylint: disable=E1101
        json.dump(
            instrument.make_dict(node), open(filename, "w"),
            sort_keys=True, indent=4)
        # pylint: enable=E1101

    @staticmethod
    def make_dict(node):
        result = {
            k: instrument.make_dict(v)
            for k, v in node.iteritems()}
        result.update({"values": node.values} if node.values else dict())
        return result

    @staticmethod
    def reset(node):
        node.values = list()
        for child_node in node.itervalues():
            instrument.reset(child_node)

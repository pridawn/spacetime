import os
import json
import time
import importlib
from abc import ABCMeta, abstractmethod
from spacetime.server.start import start_server
from benchmark.datamodel.all import DATAMODEL_TYPES, DATAMODEL_TRIGGERS
from benchmark.instrumented.instrumented_dataframe_store import instrumented_dataframe_stores

REGISTERED_TESTSUITES = list()

def register(cls):
    REGISTERED_TESTSUITES.append(cls)
    return cls


class TestCase(object):
    def __init__(self, test_module, test_suite,
                 test_name, instances, steps):
        self.test_module = test_module
        self.test_name = test_name
        self.test_suite = test_suite
        self.instances = instances
        self.steps = steps


class BaseTestSuite(object):
    __metaclass__ = ABCMeta

    @staticmethod
    def read_test_cases(testfile, test_folder):
        testcases = list()
        with open(testfile) as t_file:
            for line in t_file.readlines():
                line = line.strip()
                if not line.startswith('#'):
                    test_suite, test_name, instances, steps = line.split()
                    module = importlib.import_module(
                        "benchmark.applications.%s.%s.%s" % (
                            test_folder, test_suite, test_name))
                    testcases.append(
                        TestCase(
                            module, test_suite, test_name,
                            int(instances), int(steps)))
        return testcases

    def __init__(self, args, foldername, logger):
        self.args = args
        self.logger = logger
        dirname = os.path.dirname(
            self.__class__.__module__.replace(".", "/") + ".py")
        self.config_filename = os.path.join(
            dirname, self.__class__.__name__ + ".json")
        self.spacetime_server = True
        self.spacetime_args = None
        self.server = None
        self.server_store = None
        self.objectless_server = False
        self.load_types = list()
        self.load_triggers = list()
        self.name2class = dict()
        self.name2triggers = dict()
        self.spacetime_config = None
        self.address = None
        self.port = None
        self.foldername = foldername
        self.load_config(self.config_filename)

    def load_config(self, filename):
        config = json.load(open(filename))
        self.spacetime_server = config.setdefault("spacetime_server", False)
        self.objectless_server = config.setdefault("objectless", False)
        self.load_types = config.setdefault(
            "load_types",
            [tp.__rtypes_metadata__.name for tp in DATAMODEL_TYPES])
        self.load_triggers = config.setdefault(
            "load_triggers",
            [tr.procedure.__module__ + "." + tr.procedure.__name__
             for tr in DATAMODEL_TRIGGERS])
        self.name2class = {
            tp.__rtypes_metadata__.name: tp
            for tp in DATAMODEL_TYPES
            if tp.__rtypes_metadata__.name in set(self.load_types)}
        self.name2triggers = {
            (trigger.procedure.__module__
             + "." + trigger.procedure.__name__): trigger
            for trigger in DATAMODEL_TRIGGERS
            if (trigger.procedure.__module__
                + "."
                + trigger.procedure.__name__) in set(self.load_triggers)}
        self.address = config.setdefault("address", "127.0.0.1")
        self.port = config.setdefault("port", 12000)
        self.spacetime_config = config

    def startup(self):
        if self.spacetime_server:
            self.server_store = instrumented_dataframe_stores(
                self.name2class, self.name2triggers, self.objectless_server)
            self.server = start_server(
                self.server_store, config=self.spacetime_config)
            self.server.wait_for_start()

    @abstractmethod
    def run(self):
        pass

    def cleanup(self):
        if self.spacetime_server:
            self.server.shutdown()
            self.server_store.shutdown()
            self.server.join()
            self.logger.info("Completed test suite %s", self.__class__.__name__)
            time.sleep(100)

    def reset_server(self, instrument_filename):
        if self.spacetime_server:
            self.server.clear_store(instrument_filename)
            self.server.wait_for_reset()
import importlib
import os
from benchmark.common.base_test_suite import BaseTestSuite, TestCase, register
from benchmark.common.base_test_group import BaseTestGroup
from benchmark.common.simulation import Simulation
from benchmark.instrumented.instrumented_frame import InstrumentedFrame
from benchmark.instrumented.instrumented_connectors import \
    InstrumentedSpacetimeConnection, \
    InstrumentedObjectlessSpacetimeConnection, \
    InstrumentedMySqlConnection
from spacetime.client.declarations import Producer, GetterSetter, Tracker,\
    Deleter, Setter, Getter


class TestSuite(BaseTestSuite):
    @staticmethod
    def read_test_cases(testfile):
        testcases = list()
        with open(testfile) as t_file:
            for line in t_file.readlines():
                line = line.strip()
                if not line.startswith('#'):
                    test_suite, test_name, instances, steps = line.split()
                    module = importlib.import_module(
                        "benchmark.applications.base_tests.%s.%s" % (
                            test_suite, test_name))
                    testcases.append(
                        TestCase(
                            module, test_suite, test_name,
                            int(instances), int(steps)))
        return testcases

    def make_test_group(self, testcase):
        filename = "Server_{0}".format(testcase.test_name)
        server_instrument_filename = os.path.join(self.foldername, filename)
        test_group = BaseTestGroup(self.logger, server_instrument_filename)
        app_id_m = "Master_{0}".format(
            testcase.test_module.__name__).replace(".", "_")
        app_id_c = "Client_{0}".format(
            testcase.test_module.__name__).replace(".", "_")

        connector_m = self.connector(app_id_m, **self.kwargs)
        connector_c = self.connector(app_id_c, **self.kwargs)

        frame_m = InstrumentedFrame(connector_m, time_step=self.args.timestep)
        frame_c = InstrumentedFrame(connector_c, time_step=self.args.timestep)

        combination_m = [
            Producer(*testcase.test_module.BM_PRODUCER),
            Getter(*testcase.test_module.BM_GETTER),
            Setter(*testcase.test_module.BM_SETTER),
            GetterSetter(*testcase.test_module.BM_GETTERSETTER),
            Tracker(*testcase.test_module.BM_TRACKER),
            Deleter(*testcase.test_module.BM_DELETER)]
        combination_c = [
            Producer(*testcase.test_module.BT_PRODUCER),
            Getter(*testcase.test_module.BT_GETTER),
            Setter(*testcase.test_module.BT_SETTER),
            GetterSetter(*testcase.test_module.BT_GETTERSETTER),
            Tracker(*testcase.test_module.BT_TRACKER),
            Deleter(*testcase.test_module.BT_DELETER)]

        foldername_m = os.path.join(
            self.foldername, "Producer." + testcase.test_name)
        foldername_c = os.path.join(
            self.foldername, "Consumer." + testcase.test_name)

        master = Simulation(
            frame_m, testcase.instances, testcase.steps,
            testcase.test_module.initialize, testcase.test_module.update,
            combination_m, foldername_m, app_id_m)
        client = Simulation(
            frame_c, testcase.instances, testcase.steps,
            testcase.test_module.initialize_test,
            testcase.test_module.update_test,
            combination_c, foldername_c, app_id_c)

        test_group.add_master_sim(master)
        test_group.add_client_sim(client)
        return test_group

    def __init__(self, args, foldername, logger):
        self.test_groups = list()
        self.wait_for_server = False
        self.connector = InstrumentedSpacetimeConnection
        self.kwargs = {
            "wire_format": "json", "wait_for_server": self.wait_for_server}
        super(TestSuite, self).__init__(args, foldername, logger)

    def load_test_cases(self):
        self.test_groups = self.make_test_groups()

    def make_test_groups(self):
        testfile = os.path.join(os.path.dirname(__file__), "testlist.txt")
        testcases = TestSuite.read_test_cases(testfile)
        return [self.make_test_group(testcase) for testcase in testcases]

    def run(self):
        for test_group in self.test_groups:
            test_group.run()
            self.reset_server(test_group.server_instrument_filename)


#@register
class FullNoWaitTestSuite(TestSuite):
    def __init__(self, args, foldername, logger):
        super(FullNoWaitTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedSpacetimeConnection
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


#@register
class FullWaitTestSuite(TestSuite):
    def __init__(self, args, foldername, logger):
        super(FullWaitTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedSpacetimeConnection
        self.wait_for_server = True
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


@register
class ObjectlessNoWaitTestSuite(TestSuite):
    def __init__(self, args, foldername, logger):
        super(ObjectlessNoWaitTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedObjectlessSpacetimeConnection
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


@register
class ObjectlessWaitTestSuite(TestSuite):
    def __init__(self, args, foldername, logger):
        super(ObjectlessWaitTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedObjectlessSpacetimeConnection
        self.wait_for_server = True
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


@register
class MysqlTestSuite(TestSuite):
    def __init__(self, args, foldername, logger):
        super(MysqlTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedMySqlConnection
        self.kwargs = {
            "address": self.address,
            "user": args.user,
            "password": args.password,
            "database": args.database}

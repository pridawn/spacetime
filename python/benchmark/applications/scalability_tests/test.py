import importlib
import os
from benchmark.common.base_test_suite import BaseTestSuite, TestCase, register
from benchmark.common.base_test_group import BaseTestGroup
from benchmark.applications.scalability_tests.scalable_base_test_group import ScalableTestGroup
from benchmark.common.simulation import Simulation
from benchmark.instrumented.instrumented_frame import InstrumentedFrame
from benchmark.instrumented.instrumented_connectors import \
    InstrumentedSpacetimeConnection, \
    InstrumentedObjectlessSpacetimeConnection, \
    InstrumentedMySqlConnection
from spacetime.client.declarations import Producer, GetterSetter, Tracker,\
    Deleter, Setter, Getter

MAX_NUM_SCALING_CLIENTS = 10000


class ScalableTestSuite(BaseTestSuite):
    def generate_client(self, groupname, sim_num, testcase):
        app_id_c = "Client_{0}_{1}".format(groupname, sim_num)
        connector_c = self.connector(app_id_c, **self.kwargs)
        frame_c = InstrumentedFrame(connector_c, time_step=self.args.timestep)
        combination_c = [
            Producer(*testcase.test_module.BT_PRODUCER),
            Getter(*testcase.test_module.BT_GETTER),
            Setter(*testcase.test_module.BT_SETTER),
            GetterSetter(*testcase.test_module.BT_GETTERSETTER),
            Tracker(*testcase.test_module.BT_TRACKER),
            Deleter(*testcase.test_module.BT_DELETER)]
        foldername_c = os.path.join(
            self.foldername,
            "Consumer." + testcase.test_name + "_{0}".format(sim_num))
        return Simulation(
            frame_c, testcase.instances, testcase.steps,
            testcase.test_module.initialize_test,
            testcase.test_module.update_test,
            combination_c, foldername_c, app_id_c)


    def make_scalable_test_group(self, testcase):
        filename = "Server_{0}".format(testcase.test_name)
        server_instrument_filename = os.path.join(self.foldername, filename)
        groupname = testcase.test_name
        print("TEST: 2", self.server)
        scalable_test_group = ScalableTestGroup(
            groupname, self.logger, self.server, self.foldername,
            server_instrument_filename)
        app_id_m = "Master_{0}".format(groupname)

        connector_m = self.connector(app_id_m, **self.kwargs)

        frame_m = InstrumentedFrame(connector_m, time_step=self.args.timestep)

        combination_m = [
            Producer(*testcase.test_module.BM_PRODUCER),
            Getter(*testcase.test_module.BM_GETTER),
            Setter(*testcase.test_module.BM_SETTER),
            GetterSetter(*testcase.test_module.BM_GETTERSETTER),
            Tracker(*testcase.test_module.BM_TRACKER),
            Deleter(*testcase.test_module.BM_DELETER)]

        foldername_m = os.path.join(
            self.foldername, "Producer." + testcase.test_name)

        master = Simulation(
            frame_m, testcase.instances, testcase.steps,
            testcase.test_module.initialize, testcase.test_module.update,
            combination_m, foldername_m, app_id_m)
        client_gen = (
            self.generate_client(groupname, i, testcase)
            for i in xrange(MAX_NUM_SCALING_CLIENTS))
        scalable_test_group.add_master_sim(master)
        scalable_test_group.store_client_sim_generator(client_gen)

        return scalable_test_group

    def load_test_cases(self):
        self.test_groups = self.make_test_groups()

    def make_test_groups(self):
        testfile = os.path.join(os.path.dirname(__file__), "testlist.txt")
        testcases = ScalableTestSuite.read_test_cases(
            testfile, "scalability_tests")
        return [
            self.make_scalable_test_group(testcase) for testcase in testcases]

    def __init__(self, args, foldername, logger):
        self.test_groups = list()
        self.wait_for_server = False
        self.connector = InstrumentedSpacetimeConnection
        self.kwargs = {
            "wire_format": "json", "wait_for_server": self.wait_for_server}
        super(ScalableTestSuite, self).__init__(args, foldername, logger)

    def run(self):
        for test_group in self.test_groups:
            test_group.run()
            self.reset_server(test_group.server_instrument_filename)


# @register
class FullNoWaitScaleTestSuite(ScalableTestSuite):
    def __init__(self, args, foldername, logger):
        super(FullNoWaitScaleTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedSpacetimeConnection
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


# @register
class FullWaitScaleTestSuite(ScalableTestSuite):
    def __init__(self, args, foldername, logger):
        super(FullWaitScaleTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedSpacetimeConnection
        self.wait_for_server = True
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


@register
class ObjectlessNoWaitScaleTestSuite(ScalableTestSuite):
    def __init__(self, args, foldername, logger):
        super(ObjectlessNoWaitScaleTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedObjectlessSpacetimeConnection
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}


# @register
class ObjectlessWaitScaleTestSuite(ScalableTestSuite):
    def __init__(self, args, foldername, logger):
        super(ObjectlessWaitScaleTestSuite, self).__init__(args, foldername, logger)
        self.connector = InstrumentedObjectlessSpacetimeConnection
        self.wait_for_server = True
        self.kwargs = {
            "address": "http://{0}:{1}".format(self.address, self.port),
            "wire_format": "json", "wait_for_server": self.wait_for_server}

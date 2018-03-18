import datetime
import time
import sys
import os
from multiprocessing import Event

from benchmark.common.base_test_group import BaseTestGroup

LAUNCH_DELAY = 5
MAX_QUEUE_SIZE = 250

class ScalableTestGroup(BaseTestGroup):

    def __init__(
            self, groupname, logger, server, log_foldername,
            server_instrument_filename):
        self.master_sims = list()
        self.client_sims = list()
        self.simulations = list()

        self.master_events = set()
        self.sim_event = Event()
        self.logger = logger
        self.server = server
        self.sim_time = 0
        self.time_counter = 0

        self.need_client = True
        self.stored_master_sim = None
        self.stored_client_sim = None
        self.unprocessed_sims = list()
        self.processed_sims = list()
        self.client_gen = None
        self.log_foldername = log_foldername
        self.server_instrument_filename = server_instrument_filename
        if not os.path.exists(self.log_foldername):
            os.makedirs(self.log_foldername)
        self.logfile = os.path.join(
            self.log_foldername,
            "{0}_queue_size.txt".format(groupname))


    def add_master_sim(self, master):
        event = Event()
        master.event = event
        self.master_events.add(event)
        self.master_sims.append(master)
        self.simulations.append(master)

    def add_client_sim(self, sim):
        if sim in self.master_sims:
            raise RuntimeError(
                "Do not add simulation as both master and client simulations")
        self.simulations.append(sim)
        self.client_sims.append(sim)

    def run(self):
        try:
            for sim in self.simulations:
                sim.setup()
            self.start_master_sims()
            self.logger.info("Finished starting up all masters.")
            # Master used to check the time it takes to make requests / pulls
            self.wait_for_master()
            print "waited for Master "
            self.logger.info("All master sims are up.")
            self.logger.info("Initiating scaling sims.")
            self.start_scaling_sims()
            self.logger.info(
                "Scaling complete [Server queue overflow]. "
                "Ran %d client simulations before end.", len(self.client_sims))
            self.close_simulations()
            self.logger.info("Closed all sims.")
            self.logger.info("Stats have been written to file.")
        except KeyboardInterrupt:
            for sim in self.processed_sims:
                if sim.is_alive():
                    sim.terminate()
            sys.exit(0)

    def start_scaling_sims(self):
        queue_size = self.server.get_queue_size()
        while queue_size < MAX_QUEUE_SIZE:
            try:
                sim = self.get_new_client_sim()
            except StopIteration:
                break
            self.add_client_sim(sim)
            sim.setup()
            sim.start()
            time.sleep(LAUNCH_DELAY)
            queue_size = self.server.get_queue_size()
            self.logger.info("QUEUE SIZE IS %d", queue_size)
            print "QUEUE SIZE IS %d, APP COUNT %d" % (
                queue_size, len(self.simulations))
            open(self.logfile, "a").write(
                "%d,%d\n" % (queue_size, len(self.simulations)))

    def close_simulations(self):
        for sim in self.simulations:
            sim.close()
            sim.join()

    def store_client_sim_generator(self, client_gen):
        self.client_gen = client_gen

    def get_new_client_sim(self):
        return self.client_gen.next()

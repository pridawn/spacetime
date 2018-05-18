import datetime
import time
import sys
import os
from multiprocessing import Event
from threading import Thread
from benchmark.common.base_test_group import BaseTestGroup

LAUNCH_DELAY = 5
MAX_QUEUE_SIZE = 250

class ScalableTestGroup(BaseTestGroup):

    def __init__(
            self, groupname, logger, server, log_foldername,
            server_instrument_filename, stats_queue):
        self.master_sims = list()
        self.client_sims = list()
        self.simulations = list()
        self.master_events = set()
        self.sim_event = Event()
        self.logger = logger
        self.server = server
        self.client_gen = None
        self.log_foldername = log_foldername
        self.server_instrument_filename = server_instrument_filename
        if not os.path.exists(self.log_foldername):
            os.makedirs(self.log_foldername)
        self.logfile = os.path.join(
            self.log_foldername,
            "{0}_queue_size.txt".format(groupname))
        self.stats_queue = stats_queue

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
            print "Starting up sims"
            for sim in self.simulations:
                print "starting sim"
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
            for sim in self.simulations:
                if sim.is_alive():
                    sim.terminate()
            sys.exit(0)

    def start_scaling_sims(self):
        #queue_size = self.server.get_queue_size()
        client_to_time = dict()
        avg = [0]
        def read_q():
            while True:
                stat = self.stats_queue.get()
                appname, stats =  stat
                statname, value = stats
                client_to_time[appname] = value
                if client_to_time:
                    avg[0] = float(sum(client_to_time.values())/ len(client_to_time))

        #while queue_size < MAX_QUEUE_SIZE:
        stat_thread = Thread(target=read_q)
        stat_thread.daemon = True
        stat_thread.start()
        a = 0
        while a < 1000:
            print "Main", avg[0]
            try:
                sim = self.get_new_client_sim()
            except StopIteration:
                break
            self.add_client_sim(sim)
            sim.setup()
            sim.start()
            time.sleep(LAUNCH_DELAY)
            a = avg[0]
            print "AVG TIME IS %.3f, APP COUNT %d, " % (
                a, len(self.simulations))
            open(self.logfile, "a").write(
                 "%d,%d\n" % (a, len(self.simulations)))

    def close_simulations(self):
        for sim in self.simulations:
            sim.close()
            sim.join()

    def store_client_sim_generator(self, client_gen):
        self.client_gen = client_gen

    def get_new_client_sim(self):
        return self.client_gen.next()

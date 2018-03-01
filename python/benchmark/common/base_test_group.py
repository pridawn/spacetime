import sys
from multiprocess import Event


class BaseTestGroup(object):
    def __init__(self, logger, server_instrument_filename):
        self.master_sims = set()
        self.client_sims = set()
        self.simulations = set()
        self.master_events = set()
        self.sim_event = Event()
        self.server_instrument_filename = server_instrument_filename
        self.logger = logger

    def add_master_sim(self, master):
        event = Event()
        master.event = event
        self.master_events.add(event)
        self.master_sims.add(master)
        self.simulations.add(master)

    def add_client_sim(self, sim):
        if sim in self.master_sims:
            raise RuntimeError(
                "Do not add simulation as both master and client simulations")
        self.simulations.add(sim)
        self.client_sims.add(sim)

    def run(self):
        try:
            for sim in self.simulations:
                sim.setup()
            self.logger.info("Finished setting up all simulations.")
            self.start_master_sims()
            self.logger.info("Finished starting up all masters.")
            self.wait_for_master()
            self.logger.info("All master sims are up.")
            self.start_client_sims()
            self.logger.info("All Client sims are up.")
            self.wait_for_master()
            self.logger.info("Master sims have completed their tasks.")
            self.close_simulations()
            self.logger.info("Closed all sims.")
            self.logger.info("Stats have been written to file.")
        except KeyboardInterrupt:
            for sim in self.simulations:
                if sim.is_alive():
                    sim.terminate()
            sys.exit(0)

    def start_master_sims(self):
        for master in self.master_sims:
            master.start()

    def wait_for_master(self):
        for event in self.master_events:
            event.wait()
            event.clear()

    def start_client_sims(self):
        for sim in self.client_sims:
            sim.start()

    def close_simulations(self):
        for sim in self.simulations:
            sim.close()
            sim.join()

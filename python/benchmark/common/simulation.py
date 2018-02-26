import logging
import prctl
from multiprocess import Process
from multiprocess import Event
from multiprocess import RLock


from spacetime.client.IApplication import IApplication
from benchmark.instrumented.instrument import instrument

class BenchmarkApplication(IApplication):
    def __init__(self, frame, instances, steps, init_hook, update_hook):
        self.frame = frame
        self.done = False
        self.curstep = 0
        self.instances = instances
        self.simsteps = steps
        self.update_hook = update_hook
        self.init_hook = init_hook
        self.logger = logging.getLogger(__name__)
        self.master = False
        self.event = None

    def initialize(self):
        self.init_hook(self)
        if self.master:
            self.event.set()

    def update(self):
        if self.update_hook:
            self.update_hook(self)
        self.curstep += 1
        if self.master and self.curstep >= self.simsteps:
            self.done = True
            self.event.set()

    def shutdown(self):
        self.logger.info("Shutting down benchmark")


class Simulation(Process):
    instrument_lock = RLock()

    def __init__(
            self, frame, instances, steps, init_hook,
            update_hook, combinations, foldername, appname):
        super(Simulation, self).__init__(
            name="BENCHMARK_Simulation_{0}".format(appname))
        prctl.set_name(self.name)
        self.daemon = True
        self._event = None
        self.foldername = foldername
        self.frame = frame
        self.combinations = combinations
        self.sim = None
        self.instances = instances
        self.steps = steps
        self.init_hook = init_hook
        self.update_hook = update_hook
        self.appname = appname
        self.master = False
        self.close_event = Event()

    @property
    def event(self):
        return self._event

    @event.setter
    def event(self, value):
        self._event = value
        if value is not None:
            self.master = True

    def setup(self):
        sim_cls = BenchmarkApplication
        for comb in self.combinations:
            sim_cls = comb(sim_cls)
        self.sim = sim_cls(
            self.frame, self.instances, self.steps,
            self.init_hook, self.update_hook)
        self.frame.attach_app(self.sim, appname=self.appname)
        self.sim.master = self.master
        self.sim.event = self.event

    def run(self):
        self.frame.run_async()
        self.close_event.wait()
        self.sim.done = True
        self.frame.shutdown()
        with Simulation.instrument_lock:
            self.write_and_reset()

    def close(self):
        self.close_event.set()

    def write_and_reset(self):
        instrument.write_and_reset(self.foldername)

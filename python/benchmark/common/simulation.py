import logging
from multiprocessing import Process
from multiprocessing import Event
from multiprocessing import RLock
from multiprocessing import Queue
from multiprocessing.queues import Empty
from threading import Thread

from spacetime.client.IApplication import IApplication
from benchmark.instrumented.instrument import instrument

def create_application_class(sim_name, foldername):
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
            self.foldername = foldername
            self.sim_name = sim_name

        def initialize(self):
            self.init_hook(self)
            if self.master:
                self.event.set()

        def update(self):
            if self.update_hook:
                self.update_hook(self)
            self.curstep += 1
            if self.curstep >= self.simsteps:
                self.done = True
                if self.master:
                    self.event.set()

        def shutdown(self):
            self.logger.info("Shutting down benchmark")
    return BenchmarkApplication

class Simulation(Process):
    instrument_lock = RLock()

    def __init__(
            self, frame, instances, steps, init_hook,
            update_hook, combinations, foldername, appname, queue=None):
        super(Simulation, self).__init__(
            name="BENCHMARK_Simulation_{0}".format(appname))
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
        if queue:
            self.queue_lookup = Thread(
                target=self.write_queue, args=(queue,))
            self.queue_lookup.daemon = True
        else:
            self.queue_lookup = None
        self.shutdown = False

    @property
    def event(self):
        return self._event

    @event.setter
    def event(self, value):
        self._event = value
        if value is not None:
            self.master = True

    def write_queue(self, queue):
        print queue
        while not self.shutdown:
            for q in instrument.queues:
                try:
                    data = q.get_nowait()
                    queue.put((self.appname, data))
                except Empty:
                    continue
            

    def setup(self):
        sim_cls = create_application_class(self.appname, self.foldername)
        for comb in self.combinations:
            sim_cls = comb(sim_cls)
        self.sim = sim_cls(
            self.frame, self.instances, self.steps,
            self.init_hook, self.update_hook)
        self.frame.attach_app(self.sim, appname=self.appname)
        self.sim.master = self.master
        self.sim.event = self.event

    def run(self):
        if self.queue_lookup is None:
            instrument.no_queue = True
        else:
            self.queue_lookup.start()
        self.frame.run_async()
        while not self.close_event.wait(timeout=0.2):
            pass
        self.sim.done = True
        print "Sim done", self.appname
        self.frame.shutdown()
        self.shutdown = True
        print "writing data"
        with Simulation.instrument_lock:
            self.write_and_reset()
        print "Writing done"
        if self.queue:
            self.queue.close()
        self.terminate()

    def close(self):
        self.close_event.set()

    def write_and_reset(self):
        instrument.write_and_reset(self.foldername)

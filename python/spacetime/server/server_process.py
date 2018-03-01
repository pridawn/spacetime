import json
import logging
import os
import time
import sys
from functools import wraps
from threading import Thread
from threading import Timer

from multiprocess import Process
from multiprocess import Queue
from multiprocess import Event

import cbor
from tornado.web import RequestHandler, HTTPError
import tornado.ioloop

from spacetime.server.server_requests import RestartStoreRequest, SetUpRequest, ShutdownRequest, StartRequest
from spacetime.server.console import SpacetimeConsole

class BaseRegisterHandler(RequestHandler):
    pass


class BaseGetAllUpdatedTracked(RequestHandler):
    pass

def get_exception_handler(timers, store, logger):
    def handle_exceptions(f):
        @wraps(f)
        def wrapped(*args, **kwds):
            try:
                timers[args[1]] = time.time()
                if not isinstance(args[0], BaseRegisterHandler):
                    if args[1] not in store.get_app_list():
                        raise HTTPError(
                            500, "%s not registered to the store." % args[1])
                ret = f(*args, **kwds)
            except Exception as e:
                logger.exception(
                    "Exception %s handling function %s:", repr(e), f.func_name)
                raise HTTPError(
                    500, "Exception handling function %s:" % f.func_name)
            return ret
        return wrapped
    return handle_exceptions

def get_request_handlers(process, store, handle_exceptions):
    class GetAllUpdatedTracked(BaseGetAllUpdatedTracked):
        @handle_exceptions
        def get(self, sim):
            changelist_str = (
                self.request.body if (
                    self.request.body and store.objectless_server) else None)
            changelist = (
                cbor.loads(changelist_str)
                if changelist_str is not None else
                dict())
            data, content_type = store.getupdates(sim, changelist)
            self.set_header("content-type", content_type)
            self.write(data)

        @handle_exceptions
        def post(self, sim):
            data = self.request.body
            store.update(sim, data)


    class Register(BaseRegisterHandler):
        @handle_exceptions
        def put(self, sim):
            data = self.request.body
            json_dict = json.loads(data)
            typemap = json_dict["sim_typemap"]
            wire_format = (
                json_dict["wire_format"] if "wire_format" in json_dict else "json")
            wait_for_server = (
                json_dict["wait_for_server"]
                if "wait_for_server" in json_dict else
                False)
            store.register_app(
                sim, typemap, wire_format=wire_format,
                wait_for_server=wait_for_server)

        @handle_exceptions
        def delete(self, sim):
            process.disconnect(sim)

    return GetAllUpdatedTracked, Register

def SetupLoggers(debug) :
    if debug:
        logl = logging.INFO
    else:
        logl = logging.INFO

    logger = logging.getLogger()
    logger.setLevel(logl)
    folder = "logs/"
    if not os.path.exists(folder):
        os.mkdir(folder)
    logfile = os.path.join(folder, "frameserver.log")
    flog = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
    flog.setLevel(logl)
    flog.setFormatter(logging.Formatter(
        '%(levelname)s [%(name)s] [%(asctime)s] %(message)s'))
    logger.addHandler(flog)

    clog = logging.StreamHandler()
    clog.setFormatter(logging.Formatter('[%(name)s] [%(asctime)s] %(message)s'))
    if debug:
        clog.setLevel(logging.INFO)
    else:
        clog.setLevel(logging.INFO)
    logger.addHandler(clog)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("tornado.access").setLevel(logging.WARNING)
    return logger


class TornadoServerProcess(Process):
    '''Class that helps launch and maintain Tornado Server.'''

    @property
    def not_ready(self):
        return (
            self.logger is None
            or self.store is None
            or self.timers is None
            or self.app is None)

    def __init__(self):
        super(TornadoServerProcess, self).__init__(
            name="BENCHMARK_TornadoServerProcess")
        self.daemon = True
        self.work_queue = Queue()
        self.done = False
        self.logger = None
        self.store = None
        self.timers = None
        self.app = None
        self.port = None
        self.timeout = 0
        self.disconnect_timer = None
        self.app_thread = Thread(
            target=tornado.ioloop.IOLoop.current().start,
            name="Thread_TornadoServer")
        self.app_thread.daemon = True
        self.start_event = Event()
        self.reset_event = Event()

    #################################################
    # APIs not in the same process as run
    #################################################

    def setup(self, debug, store, timeout=0):
        self.work_queue.put(SetUpRequest(debug, store, timeout))

    def start_server(self, port, console):
        stdin = sys.stdin
        self.work_queue.put(StartRequest(
            port, console, stdin))

    def restart_store(self, instrument_filename=None):
        self.work_queue.put(RestartStoreRequest(instrument_filename))

    def shutdown(self):
        self.work_queue.put(ShutdownRequest())

    def wait_for_start(self):
        self.start_event.wait()
        self.start_event.clear()

    def wait_for_reset(self):
        self.reset_event.wait()
        self.reset_event.clear()

    #################################################
    # APIs in the same process as run
    #################################################

    def run(self):
        while not self.done:
            try:
                req = self.work_queue.get()
                if isinstance(req, SetUpRequest):
                    self.process_setup(req)
                elif isinstance(req, StartRequest):
                    self.process_start(req)
                elif isinstance(req, RestartStoreRequest):
                    self.process_restart_store(req)
                elif isinstance(req, ShutdownRequest):
                    self.process_shutdown()
            except KeyboardInterrupt:
                self.process_shutdown()

    def process_setup(self, req):
        self.logger = SetupLoggers(req.debug)
        self.logger.info("Log level is " + str(self.logger.level))
        self.store = req.store
        self.timers = dict()
        self.timeout = req.timeout
        handle_exceptions = get_exception_handler(
            self.timers, self.store, self.logger)
        get_all_updated_tracked, register = get_request_handlers(
            self, self.store, handle_exceptions)
        self.app = tornado.web.Application([
            (r"/([a-zA-Z0-9_-]+)/updated", get_all_updated_tracked),
            (r"/([a-zA-Z0-9_-]+)", register)])

    def process_start(self, req):
        if self.not_ready:
            raise RuntimeError(
                "Trying to start tornado server without setting it up first.")
        self.port = req.port
        self.store.start()

        if req.console:
            console = SpacetimeConsole(self.store, self, stdin=req.stdin)
            con_thread = Thread(
                target=console.cmdloop,
                name="Thread_spacetime_console")
            con_thread.daemon = True
            con_thread.start()

        if self.timeout > 0:
            self.start_timer()

        self.app.listen(self.port)
        self.app_thread.start()
        self.start_event.set()

    def process_restart_store(self, req):
        if self.not_ready:
            raise RuntimeError(
                "Trying to restart tornado server without setting it up first.")
        if req.instrument_filename:
            self.store.save_instrumentation_data(req.instrument_filename)
        self.store.clear()
        self.reset_event.set()

    def process_shutdown(self):
        self.store.shutdown()
        tornado.ioloop.IOLoop.instance().stop()
        self.done = True

    def start_timer(self):
        self.disconnect_timer = Timer(self.timeout, self.check_disconnect, ())
        self.disconnect_timer.start()

    def check_disconnect(self):
        if not self.store.pause_servers:
            for sim in self.timers:
                if (time.time() - self.timers[sim]) > self.timeout:
                    self.disconnect(sim)
        self.start_timer()

    def disconnect(self, sim):
        self.store.gc(sim)
        del self.timers[sim]

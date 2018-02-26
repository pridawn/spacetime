class ServerRequest(object):
    pass


class SetUpRequest(ServerRequest):
    def __init__(self, debug, store, timeout):
        self.debug = debug
        self.store = store
        self.timeout = timeout


class StartRequest(ServerRequest):
    def __init__(self, port, console, stdin):
        self.port = port
        self.console = console
        self.stdin = stdin


class RestartStoreRequest(ServerRequest):
    pass


class ShutdownRequest(ServerRequest):
    pass

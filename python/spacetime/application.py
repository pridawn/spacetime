from uuid import uuid4
from multiprocessing.dummy import Process

from spacetime.dataframe import Dataframe

def get_details(dataframe):
    if isinstance(dataframe , Dataframe):
        return dataframe.details
    elif isinstance(dataframe, tuple):
        return dataframe
    raise RuntimeError(
        "Do not know how to connect to dataframe with given data")

def get_app(func, types, producer,
            getter_setter, getter, setter, deleter):

    class Application(Process):
        @property
        def type_map(self):
            return {
                "producer": self.producer,
                "gettersetter": self.getter_setter,
                "getter": self.getter,
                "setter": self.setter,
                "deleter": self.deleter,
            }

        @property
        def all_types(self):
            return self.producer.union(
                self.getter_setter).union(
                    self.getter).union(
                        self.setter).union(
                            self.deleter).union(self.types)

        def __init__(self, dataframe=None, server_port=0):
            self.appname = "{0}_{1}".format(func.__name__, str(uuid4()))
            self.producer = producer
            self.getter_setter = getter_setter
            self.getter = getter
            self.setter = setter
            self.deleter = deleter
            self.types = types

            self.func = func
            self.args = tuple()
            self.kwargs = dict()
            self.dataframe_details = get_details(dataframe) if dataframe else None
            self.server_port = server_port

            super().__init__()
            self.daemon = True

        def run(self):
            # Create the dataframe.
            dataframe = self._create_dataframe(self.dataframe_details, server_port=self.server_port)
            # Fork the dataframe for initialization of app.
            dataframe.fork()
            # Run the main function of the app.
            self.func(dataframe, *self.args, **self.kwargs)
            # Merge the final changes back to the dataframe.
            dataframe.join()
            dataframe.push()

        def _start(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs
            super().start()
    
        def start(self, *args, **kwargs):
            self._start(*args, **kwargs)
            self.join()

        def start_async(self, *args, **kwargs):
            self._start(*args, **kwargs)

        def _create_dataframe(self, details, server_port=0):
            df = Dataframe(self.appname, self.all_types, details=details, server_port=server_port)
            #print(self.appname, self.all_types, details, df.details)
            return df
    return Application


class app(object):
    def __init__(
            self, Types=list(), Producer=list(), GetterSetter=list(),
            Getter=list(), Setter=list(), Deleter=list()):
        self.producer = set(Producer)
        self.getter_setter = set(GetterSetter)
        self.getter = set(Getter)
        self.setter = set(Setter)
        self.deleter = set(Deleter)
        self.types = set(Types)

    def __call__(self, func):
        return get_app(
            func, self.types, self.producer, self.getter_setter,
            self.getter, self.setter, self.deleter)

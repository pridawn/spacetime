from multiprocessing import RLock
import traceback

from spacetime.managers.socket_manager import SocketServer, SocketConnector
from spacetime.managers.version_manager import FullStateVersionManager
from spacetime.managers.managed_heap import ManagedHeap
from spacetime.managers.diff import Diff
import spacetime.utils.enums as enums
import spacetime.utils.utils as utils


class Dataframe(object):
    @property
    def details(self):
        return self.socket_server.port

    def __init__(self, appname, types, worker_count=5, details=None, server_port=0):
        self.appname = appname
        self.logger = utils.get_logger("%s_Dataframe" % appname)
        self.socket_server = SocketServer(
            self.appname, details, server_port, worker_count,
            self.pull_call_back, self.push_call_back, self.confirm_pull_req)

        self.socket_connector = SocketConnector(
            self.appname, details, self.details)

        self.types = types
        self.type_map = {
            tp.__r_meta__.name: tp for tp in self.types}
        self.local_heap = ManagedHeap(types)
        self.versioned_heap = FullStateVersionManager(self.appname, types)
        self.write_lock = RLock()
        
        self.socket_server.start()
        if self.socket_connector.has_parent_connection:
            self.pull()

    # Suppport Functions

    def _create_package(self, appname, diff, start_version):
        return appname, [start_version, diff.version], diff

    # Object Create Add and Delete
    def add_one(self, dtype, obj):
        '''Adds one object to the staging.'''
        self.local_heap.add_one(dtype, obj)

    def add_many(self, dtype, objs):
        '''Adds many objects to the staging.'''
        self.local_heap.add_many(dtype, objs)

    def read_one(self, dtype, oid):
        '''Reads one object either from staging or
           last forked version if it exists.
           Returns None if no object is found.'''
        return self.local_heap.read_one(dtype, oid)

    def read_all(self, dtype):
        '''Returns a list of all objects of given type either
           from staging or last forked version.
           Returns empty list if no objects are found.'''
        return self.local_heap.read_all(dtype)

    def delete_one(self, dtype, obj):
        '''Deletes obj from staging first. If it exists
           in previous version, adds a delete record.'''
        self.local_heap.delete_one(dtype, obj)

    def delete_all(self, dtype):
        self.local_heap.delete_all(dtype)

    # Fork and Join

    def checkout(self):
        data, versions = self.versioned_heap.retrieve_data(
            self.appname,
            self.local_heap.version)
        if versions[0] != versions[1]:
            if self.local_heap.receive_data(data, versions[1]):
                # Can be carefully made Async.
                with self.write_lock:
                    self.versioned_heap.data_sent_confirmed(
                        self.appname, versions[1])

    def commit(self):
        data, versions = self.local_heap.retreive_data()
        with self.write_lock:
            succ = self.versioned_heap.receive_data(
                self.appname, versions, data)
        if succ and versions[0] != versions[1]:
            self.local_heap.data_sent_confirmed()

    def sync(self):
        self.commit()
        if self.socket_connector.has_parent_connection:
            self.push()
            self.pull()
        self.checkout()
        return True

    # Push and Pull
    def push(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Push request started.")
            with self.write_lock:
                data, version = self.versioned_heap.retrieve_data(
                    "SOCKETPARENT", self.socket_connector.parent_version)
                if version[0] == version[1]:
                    self.logger.debug(
                        "Push not required, "
                        "parent already has the information.")
                    return
            if self.socket_connector.push_req(data, version):
                self.logger.debug("Push request completed.")
                with self.write_lock:
                    self.versioned_heap.data_sent_confirmed(
                        "SOCKETPARENT", version[1])
                self.logger.debug("Push request registered.")

    def pull(self):
        if self.socket_connector.has_parent_connection:
            self.logger.debug("Pull request started.")
            package, version = self.socket_connector.pull_req()
            self.logger.debug("Pull request completed.")
            with self.write_lock:
                self.versioned_heap.receive_data(
                    "SOCKETPARENT",
                    version, package)
            self.logger.debug("Pull request applied.")

    # Functions that respond to external requests

    def pull_call_back(self, appname, version):
        try:
            with self.write_lock:
                data = self.versioned_heap.retrieve_data(appname, version)
                return data
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise
    
    def confirm_pull_req(self, appname, version):
        try:
            if version[0] != version[1]:
                with self.write_lock:
                    self.versioned_heap.data_sent_confirmed(
                        appname, version[1])
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise
            
    def push_call_back(self, appname, versions, data):
        try:
            with self.write_lock:
                data = self.versioned_heap.receive_data(
                    appname, versions, data)
                return data
        except Exception as e:
            print (e)
            print(traceback.format_exc())
            raise


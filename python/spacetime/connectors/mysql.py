from __future__ import absolute_import
from rtypes.connectors.sql import RTypesMySQLConnection
from rtypes.pcc.utils.enums import Event
import itertools
import logging
from logging import NullHandler
from spacetime.common.modes import Modes

class MySqlConnection(object):
    def __init__(self, user, password, database):
        self.user = user
        self.password = password
        self.database = database
        self.host_to_connection = dict()
        self.host_to_wire_format = dict()
        self.host_to_typemap = dict()
        self.host_to_pccmap = dict()
        self.host_to_read_types = dict()
        self.host_to_all_types = dict()

    def __setup_logger(self, name, file_path=None):
        logger = logging.getLogger(name)
        logger.addHandler(NullHandler())
        logger.setLevel(logging.DEBUG)
        logger.debug("Starting logger for %s",name)
        return logger

    def add_host(self, app_id, host, wire_format, typemap):
        self.logger = self.__setup_logger(
            "spacetime-connector" + app_id)

        self.host_to_connection[host] = None
        self.host_to_wire_format[host] = wire_format
        self.host_to_typemap[host] = typemap
        all_types = list()
        for mode in typemap:
            if mode != Modes.Triggers:
                all_types.extend(typemap[mode])
        read_types = list()
        for mode in typemap:
            if mode not in set([Modes.Triggers,
                                Modes.Deleter,
                                Modes.Producing]):
                read_types.extend(typemap[mode])
        pcc_map = {
            tp.__rtypes_metadata__.name: tp
            for tp in all_types}

        self.host_to_pccmap[host] = pcc_map
        self.host_to_read_types[host] = read_types
        self.host_to_all_types[host] = all_types

    def register(self, app_id, host):
        self.host_to_connection[host] = RTypesMySQLConnection(
            user=self.user, password=self.password,
            host=host, database=self.database)
        connection = self.host_to_connection[host]
        all_types = self.host_to_all_types[host]
        pcc_map = self.host_to_pccmap[host]
        connection.__rtypes_write__({
            "types": {
                tp.__rtypes_metadata__.name: Event.New
                for tp in all_types}}, pcc_map)
        return True

    def update(self, app_id, host, changes):
        connection = self.host_to_connection[host]
        pcc_map = self.host_to_pccmap[host]
        connection.__rtypes_write__(changes, pcc_map)
        return True

    def get_updates(self, app_id, host):
        read_types = self.host_to_read_types[host]
        connection = self.host_to_connection[host]
        results, not_diff = connection.__rtypes_query__(read_types)
        return True, len(results), not not_diff, results

    def disconnect(self, app_id, host):
        connection = self.host_to_connection[host]
        all_types = self.host_to_all_types[host]
        pcc_map = self.host_to_pccmap[host]
        connection.__rtypes_write__({
            "types": {
                tp.__rtypes_metadata__.name: Event.Delete
                for tp in all_types}}, pcc_map)

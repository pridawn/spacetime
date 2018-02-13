from __future__ import absolute_import
from rtypes.connectors.sql import RTypesMySQLConnection
from rtypes.pcc.utils.enums import Event
import itertools
import requests
import json
import sys
import os
import dill
import cbor
import logging
import platform
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from spacetime.common.javahttpadapter import MyJavaHTTPAdapter, ignoreJavaSSL
from rtypes.pcc.triggers import TriggerProcedure
from spacetime.common.wire_formats import FORMATS
from logging import NullHandler
from requests.exceptions import HTTPError, ConnectionError
from spacetime.common.modes import Modes

class SpacetimeConnection(object):
    def __init__(self, wait_for_server=False):
        self.host_to_connection = dict()
        self.host_to_wire_format = dict()
        self.host_to_typemap = dict()
        self.host_to_pccmap = dict()
        self.host_to_read_types = dict()
        self.host_to_all_types = dict()
        self.wait_for_server = wait_for_server

    def __setup_logger(self, name, file_path=None):
        logger = logging.getLogger(name)
        logger.addHandler(NullHandler())
        logger.setLevel(logging.DEBUG)
        logger.debug("Starting logger for %s",name)
        return logger

    def __handle_request_errors(self, resp):
        if resp.status_code == 401:
            self.logger.error(
                "This application is not registered at the server. Stopping..")
            raise RuntimeError(
                "This application is not registered at the server.")
        else:
            self.logger.warn(
                "Non-success code received from server: %s %s",
                resp.status_code, resp.reason)

    def add_host(self, app_id, hostpart, wire_format, typemap):
        host = hostpart + app_id
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

    def register(self, app_id, hostpart):
        host = hostpart + app_id
        jobj = {
            k: [tp.name
                if isinstance(tp, TriggerProcedure) else
                tp.__rtypes_metadata__.name for tp in v]
            for k, v in self.host_to_typemap[host].iteritems()}
        wire_format = self.host_to_wire_format[host]
        jsonobj = json.dumps(
            {"sim_typemap": jobj, "wire_format": wire_format,
                "app_id": app_id, "wait_for_server": self.wait_for_server})
        try:
            self.host_to_connection[host] = Session()
            if platform.system() == "Java":
                ignoreJavaSSL()
                self.logger.info("Using custom HTTPAdapter for Jython")
                self.host_to_connection[host].mount(
                    host, MyJavaHTTPAdapter())
                self.host_to_connection[host].verify=False
            self.logger.info("Registering with %s", host)
            resp = requests.put(
                host,
                data=jsonobj,
                headers={"content-type": "application/octet-stream"})
        except HTTPError:
            self.__handle_request_errors(resp)
            return False
        except ConnectionError:
            self.logger.exception("Cannot connect to host.")
            return False

    def update(self, app_id, hostpart, changes):
        host = hostpart + app_id
        try:
            _, content_type = FORMATS[self.host_to_wire_format[host]]
            dictmsg = changes.SerializeToString()
            headers = {"content-type": content_type}
            resp = self.host_to_connection[host].post(host + "/updated", 
                                                      data=dictmsg, 
                                                      headers=headers)
        except TypeError:
            self.logger.exception(
                "error encoding obj. Object: %s", changes)
        except HTTPError:
            self.__handle_request_errors(resp)
        except ConnectionError:
            return False
        return True

    def get_updates(self, app_id, hostpart):
        host = hostpart + app_id
        resp = self.host_to_connection[host].get(host + "/updated", data = {})
        DF_CLS, _ = (
            FORMATS[self.host_to_wire_format[host]])
        dataframe_change = DF_CLS()
        try:
            resp.raise_for_status()
            data = resp.content
            dataframe_change.ParseFromString(data)
            return True, len(resp.content), True, dataframe_change
        except HTTPError:
            self.__handle_request_errors(resp)
        except ConnectionError:
            pass
        return False, 0, True, dataframe_change

    def disconnect(self, app_id, hostpart):
        host = hostpart + app_id
        _ = requests.delete(host)
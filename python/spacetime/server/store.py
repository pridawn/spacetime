'''
Created on Apr 19, 2016
@author: Rohan Achar
'''

import cbor
import hashlib
import importlib
import inspect
import os
from time import sleep
import sys, traceback

from rtypes.dataframe.dataframe_threading import dataframe_wrapper as dataframe_t
from rtypes.dataframe.dataframe import dataframe
from rtypes.dataframe.objectless_dataframe import ObjectlessDataframe as dataframe_ol
from rtypes.dataframe.application_queue import ApplicationQueue
from rtypes.pcc.utils.enums import Record

from spacetime.common.wire_formats import FORMATS
from spacetime.common.modes import Modes
from spacetime.common.converter import create_jsondict, create_complex_obj
from spacetime.common.crawler_generator import generate_datamodel

FETCHING_MODES = set([Modes.Getter, 
                      Modes.GetterSetter,
                      Modes.Taker])
TRACKING_MODES = set([Modes.Tracker])
PUSHING_MODES = set([Modes.Deleter,
                     Modes.GetterSetter,
                     Modes.Setter,
                     Modes.TakerSetter,
                     Modes.Producing])
ALL_MODES = set([Modes.Deleter,
                 Modes.GetterSetter,
                 Modes.Setter,
                 Modes.TakerSetter,
                 Modes.Producing,
                 Modes.Tracker,
                 Modes.Getter])

CRAWLER_SAVE_FILE = "spacetime_crawler_data"

class dataframe_stores(object):
    @property
    def is_alive(self):
        return self.master_dataframe.isAlive()

    def __init__(self, name2class, name2triggers, objectless_server):
        self.objectless_server = objectless_server
        self.master_dataframe = None
        self.app_to_df = dict()
        self.name2class = name2class
        self.name2triggers = name2triggers
        self.pause_servers = False
        self.app_wire_format = dict()
        self.app_wait_for_server = dict()
        self.instrument_filename = None
        self.app_to_stats = dict()
        # self.master_dataframe.add_types(self.name2class.values())

    def __pause(self):
        while self.pause_servers:
            sleep(0.1)

    def start(self):
        self.master_dataframe = dataframe_t(
            dataframe=(
                dataframe()
                if not self.objectless_server else
                dataframe_ol()))
        self.master_dataframe.start()

    def add_new_dataframe(self, name, df):
        self.__pause()
        self.app_to_df[name] = df

    def delete_app(self, app):
        self.__pause()
        del self.app_to_df[app]

    def load_all_sets(self, app_name):
        # make all the stuff
        app_id = app_name.split("_")[-1]
        uas, filename, typenames = generate_datamodel(app_id)
        classes = list()
        mod = importlib.import_module("datamodel.search." + filename + "_datamodel")
        reload(mod)
        for name, cls in inspect.getmembers(mod):
            if hasattr(cls, "__rtypes_metadata__"):
               self.name2class[cls.__rtypes_metadata__.name] = cls
        return {
            Modes.Producing: set([self.name2class["datamodel.search.{0}_datamodel.{0}Link".format(app_id)]]),
            Modes.GetterSetter: set([self.name2class["datamodel.search.{0}_datamodel.One{0}UnProcessedLink".format(app_id)]])
            }

    def parse_type(self, app, mode_map):
        # checks if the tpname is in dataframe yet
        if all(tpname in self.name2class
               for mode, mode_types in mode_map.iteritems()
               for tpname in mode_types):
            return {mode: [self.name2class[tpname] for tpname in mode_types]
                    for mode, mode_types in mode_map.iteritems()}
        else:
            # builds the appropriate types and files
            return self.load_all_sets(app)
        return tp

    def register_app(self, app, type_map,
                     wire_format="json", wait_for_server=False):
        self.__pause()
        # TODO: Make a way to add types here
        # used this throughout the function instead of "type_map"
        real_type_map = self.parse_type(app, type_map)

        # Add all types to master.
        types_to_add_to_master = set()
        for mode in ALL_MODES:
            types_to_add_to_master.update(
                set(real_type_map.setdefault(mode, set())))


        all_types = [self.name2class[str(tpam.__rtypes_metadata__)] for tpam in types_to_add_to_master]
        self.master_dataframe.add_types(all_types)

        # Look at invidual types.
        if not self.objectless_server:
            types_to_get = set()
            for mode in FETCHING_MODES:
                types_to_get.update(set(type_map.setdefault(mode, set())))
            types_to_track = set()
            for mode in TRACKING_MODES:
                types_to_track.update(set(type_map.setdefault(mode, set())))
            types_to_track = types_to_track.difference(types_to_get)
            real_types_to_get = [self.name2class[tpg] for tpg in types_to_get]
            real_types_to_track = [
                self.name2class[tpt] for tpt in types_to_track]

            df = ApplicationQueue(
                app, real_types_to_get + real_types_to_track,
                self.master_dataframe)
            self.add_new_dataframe(app, df)
        else:
            # Just required for the server to not disconnect apps not registered
            self.app_to_df[app] = None
            try:
                state_manager = self.master_dataframe.dataframe.state_manager
                undownloaded = (
                    len(state_manager.type_to_objids[
                        "datamodel.search.{0}_datamodel."
                        "{0}UnprocessedLink".format(app)]))
                total = (len(state_manager.type_to_objids[
                        "datamodel.search.{0}_datamodel.{0}Link".format(app)]))
                self.app_to_stats[app] = (total - undownloaded, undownloaded)
            except KeyError:
                print "First time entry for app", app
                self.app_to_stats[app] = (0, 0)
        # Adding to name2class
        for tp in all_types:
            self.name2class.setdefault(tp.__rtypes_metadata__.name, tp)

        # setting the wire format for the app.
        self.app_wire_format[app] = wire_format

        # Setting interaction mode.
        self.app_wait_for_server[app] = wait_for_server

    def disconnect(self, app):
        self.__pause()
        if app in self.app_to_df:
            self.delete_app(app)

    def reload_dms(self, datamodel_types):
        self.__pause()
        pass

    # spacetime automatically pushing changes into server
    def update(self, app, changes, callback=None):
        try:
            self.__pause()
            dfc_type, _ = FORMATS[self.app_wire_format[app]]
            dfc = dfc_type()
            dfc.ParseFromString(changes)
            # print "DFC :::: ", dfc
            downloaded, undownloaded = self.app_to_stats[app]
            group_tpname = "datamodel.search.{0}_datamodel.{0}Link".format(app)
            if group_tpname in dfc["gc"]:
                for link_key, obj_changes in dfc['gc'][group_tpname].iteritems():
                    if obj_changes["dims"]["download_complete"]["value"]:
                        downloaded += 1
                        undownloaded -= 1
                        link_as_file = self.make_link_into_file(link_key)
                        if not os.path.exists(link_as_file):
                            # add the data to the file
                            link_data = {
                                dimname: dimchange
                                for dimname, dimchange in (
                                    obj_changes["dims"].iteritems())
                                if dimname is not "download_complete"}
                            
                            cbor.dump(link_data, open(link_as_file, "wb"))
                            print "WRITES THE FILE"

                        new_data = {
                            "download_complete": {
                                "type": Record.BOOL, "value": True}}
                        if "error_reason" in obj_changes["dims"]:
                            new_data["error_reason"] = (
                                obj_changes["dims"]["error_reason"])
                        else:
                            new_data["error_reason"] = {
                                "type": Record.STRING, "value": ""}
                        obj_changes["dims"] = new_data
                    else:
                        undownloaded += 1

                self.app_to_stats[app] = (downloaded, undownloaded)
            if app in self.app_to_df:
                self.master_dataframe.apply_changes(
                    dfc, except_app=app,
                    wait_for_server=self.app_wait_for_server[app])
            # before this
            if callback:
                callback(app)
        except Exception, e:
            print "U ERROR!!!", e, e.__class__.__name__
            raise

    # thier pull into client
    def getupdates(self, app, changelist=None, callback=None):
        """ The client is pulling info from the server """
        try:
            self.__pause()
            dfc_type, content_type = FORMATS[self.app_wire_format[app]]
            final_updates = dfc_type()
            if self.objectless_server:
                # change this before callback
                final_updates = dfc_type(
                    self.master_dataframe.get_record(changelist, app))

                group_tpname = "datamodel.search.{0}_datamodel.{0}Link".format(app)
                if group_tpname in final_updates["gc"]:
                    for link_key in final_updates['gc'][group_tpname]:
                        if "dims" in final_updates['gc'][group_tpname][link_key]:
                            self.check_base_dir_for_crawler_data()
                            link_as_file = self.make_link_into_file(link_key)
                            if os.path.exists(link_as_file):
                                # this means that the data already exist on disk
                                # so grab the data rather than downloading it
                                data = cbor.load(open(link_as_file, "rb"))
                                final_updates['gc'][group_tpname][link_key]["dims"].update(data)
                final_updates["stats"] = self.app_to_stats[app]
            else:
                if app in self.app_to_df:
                    final_updates = dfc_type(self.app_to_df[app].get_record())
                    self.app_to_df[app].clear_record()
            if callback:
                callback(app, final_updates.SerializeToString(), content_type)
            else:
                return final_updates.SerializeToString(), content_type
        except Exception, e:
            print "GU ERROR!!!", e, e.__class__.__name__
            ex_type, ex, tb = sys.exc_info()
            traceback.print_tb(tb)
            raise

    def check_base_dir_for_crawler_data(self):
        """ Make sure that the base dir used to stor the crawler data exist """
        if not os.path.exists(CRAWLER_SAVE_FILE):
            os.makedirs(CRAWLER_SAVE_FILE)

    def make_link_into_file(self, link):
        try:
            hashed_link = hashlib.sha224(link).hexdigest()
        except UnicodeEncodeError:
            try:
                hashed_link = hashlib.sha224(link.encode("utf-8")).hexdigest()
            except UnicodeEncodeError:
                hashed_link = str(hash(link))
        
        return os.path.join(CRAWLER_SAVE_FILE, hashed_link)

    def make_hashed_link_into_file(self, hashed_link):
        """ Make the hashed link into a file in the base dir """
        





















    def get_app_list(self):
        return self.app_to_df.keys()

    def clear(self, tp=None):
        if not tp:
            self.shutdown()
            print "Restarting master dataframe."
            self.__init__(
                self.name2class, self.name2triggers, self.objectless_server)
            self.start()
        else:
            if tp in self.master_dataframe.object_map:
                del self.master_dataframe.object_map[tp]
            if tp in self.master_dataframe.current_state:
                del self.master_dataframe.current_state[tp]

    def shutdown(self):
        print "Shutting down master dataframe."
        if self.master_dataframe:
            self.master_dataframe.shutdown()
            self.master_dataframe.join()
        print "Master dataframe has shut down."

    def pause(self):
        self.pause_servers = True

    def unpause(self):
        self.pause_servers = False

    def gc(self, sim):
        # For now not clearing contents
        self.delete_app(sim)

    def get(self, tp):
        return [create_jsondict(o) for o in self.master_dataframe.get(tp)]

    def put(self, tp, objs):
        real_objs = [
            create_complex_obj(tp, obj, self.master_dataframe.object_map)
            for obj in objs.values()]
        tpname = tp.__rtypes_metadata__.name
        gkey = self.master_dataframe.member_to_group[tpname]
        if gkey == tpname:
            self.master_dataframe.extend(tp, real_objs)
        else:
            for obj in real_objs:
                oid = obj.__primarykey__
                if oid in self.master_dataframe.object_map[gkey]:
                    # do this only if the object is already there.
                    # cannot add an object if it is a subset
                    # (or any other pcc type) type if it doesnt exist.
                    for dim in obj.__dimensions__:
                        # setting attribute to the original object,
                        # so that changes cascade
                        setattr(
                            self.master_dataframe.object_map[gkey][oid],
                            dim._name, getattr(obj, dim._name))

    def save_instrumentation_data(self):
        pass

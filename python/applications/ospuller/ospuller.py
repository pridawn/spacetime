'''
Created on May 16, 2016

@author: Arthur Valadares
'''
import copy
import logging
import os
from random import choice

from datamodel.common.datamodel import Vehicle
from datamodel.nodesim.datamodel import Vector3
from spacetime_local import IApplication
from spacetime_local.declarations import Getter, Tracker
import OpenSimRemoteControl
import sys
import time

import uuid
import json
from uuid import UUID

def fetch_assets(host, user, pwd, table, fpath):
    import MySQLdb
    assets = {}
    assets["Sedan"] = {}
    assets["Truck"] = {}
    assets["Taxi"] = {}

    # Open database connection
    db = MySQLdb.connect(host, user, pwd, table)

    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    # execute SQL query using execute() method.
    cursor.execute("select inventoryName, assetID from inventoryitems where inventoryName like '%sedan%'")

    # Fetch a single row using fetchone() method.
    sedan_data = cursor.fetchall()
    for tup in sedan_data:
        assets["Sedan"][tup[0]] = tup[1]

    # execute SQL query using execute() method.
    cursor.execute("select inventoryName, assetID from inventoryitems where inventoryName like '%truck%'")

    truck_data = cursor.fetchall()
    for tup in truck_data:
        assets["Truck"][tup[0]] = tup[1]

    # execute SQL query using execute() method.
    cursor.execute("select inventoryName, assetID from inventoryitems where inventoryName like '%taxi%'")

    taxi_data = cursor.fetchall()
    for tup in taxi_data:
        assets["Taxi"][tup[0]] = tup[1]


    base_path = os.path.dirname(os.path.realpath(__file__))
    final_path = os.path.join(base_path, fpath)

    with open(final_path, 'w') as outfile:
        json.dump(assets, outfile)

    # disconnect from server
    db.close()

@Getter(Vehicle)
class OpenSimPuller(IApplication.IApplication):
    def __init__(self, frame):
        self.frame = frame
        self.logger = logging.getLogger(__name__)
        #self.endpoint = "http://ucigridb.nacs.uci.edu:9000/Dispatcher/"
        self.endpoint = "http://parana.mdg.lab:9000/Dispatcher/"
        self.lifespan = 3600000
        #self.avname = "Arthur Valadares"
        #self.passwd = "arthur123"
        self.avname = "Test User"
        self.passwd = "test"
        self.scene_name = "City00"
        base_path = os.path.dirname(os.path.realpath(__file__))
        final_path = os.path.join(base_path, 'data/assets_niagara.js')
        self.assets = json.load(open(final_path))
        #fetch_assets("niagara.ics.uci.edu", "opensim", "opensim", "opensim", "data/assets_niagara.js")

        self.carids = {}

    def initialize(self):
        self.AuthByUserName()

    def update(self):
        new_vehicles = self.frame.get_new(Vehicle)
        #print "new:", new_vehicles
        for v in new_vehicles:
            assetid = UUID(choice(self.assets["Sedan"].values()))
            result = self.rc.CreateObject(
                assetid, objectid=v.ID, name=v.Name, async=True,
                pos = [v.Position.X, v.Position.Y, v.Position.Z],
                vel = [v.Velocity.X, v.Velocity.Y, v.Velocity.Z])
            self.logger.info("New vehicle: %s", v.ID)
            #if not result:
            #    self.logger.error("could not create vehicle %s", v.ID)

        mod_vehicles = self.frame.get_mod(Vehicle)
        update_list = []
        for v in mod_vehicles:
            #self.logger.info("Vehicle %s is in %s", v.ID, v.Position)
            vpos = [v.Position.X, v.Position.Y, v.Position.Z]
            vvel = [v.Velocity.X, v.Velocity.Y, v.Velocity.Z]
            vrot = [0, 0, 0, 1] # TODO: Calculate rotation
            update_list.append(OpenSimRemoteControl.BulkUpdateItem(v.ID, vpos, vvel, vrot))
            #if not result:
            #    self.logger.error("error updating vehicle %s", v.ID)

        del_vehicles = self.frame.get_deleted(Vehicle)
        for v in del_vehicles:
            self.logger.info("Deleting vehicle %s", v.ID)
            result = self.rc.DeleteObject(v.ID, async=False)

        result = self.rc.BulkDynamics(update_list, False)
        #print "result is ", result

    def shutdown(self):
        for v in self.frame.get(Vehicle):
            result = self.rc.DeleteObject(v.ID, async=False)
            if not result:
                self.logger.warn("could not clean up vehicle %s", v.ID)

    def AuthByUserName(self):
        rc = OpenSimRemoteControl.OpenSimRemoteControl(self.endpoint)
        rc.DomainList = ['Dispatcher', 'RemoteControl']
        response = rc.AuthenticateAvatarByName(self.avname,self.passwd,self.lifespan)
        if not response['_Success'] :
            print 'Failed: ' + response['_Message']
            sys.exit(-1)

        expires = response['LifeSpan'] + int(time.time())
        print >> sys.stderr, 'capability granted, expires at %s' % time.asctime(time.localtime(expires))

        print "Capability of %s is %s" % (self.scene_name,response['Capability'])
        self.capability = response['Capability'].encode('ascii')
        self.lifespan = response['LifeSpan']

        rc.Capability = uuid.UUID(self.capability)
        rc.Scene = self.scene_name
        rc.Binary = True
        self.rc = rc
        return True
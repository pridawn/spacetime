import time
import datetime
from uuid import uuid4
from rtypes import pcc_set, primarykey, dimension, merge

@pcc_set
class Register(object):
    appname = dimension(str)
    port = dimension(int)

    def __init__(self, appname):
        self.appname = appname
        self.port = 0

@pcc_set
class CheckoutObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Checkout", appname, str(uuid4()))
        self.appname = appname
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time

@pcc_set
class CommitObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Commit", appname, str(uuid4()))
        self.appname = appname
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time

@pcc_set
class PullObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Pull", appname, str(uuid4()))
        self.appname = appname
        #self.parent_appname = parent_appname
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time

@pcc_set
class PushObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Push", appname, str(uuid4()))
        self.appname = appname
        #self.parent_appname = parent_appname
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time

@pcc_set
class AcceptPullObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Accept Pull", appname, str(uuid4()))
        self.appname = appname
        #self.requestor = requestor
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time

@pcc_set
class ConfirmPullObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Confirm Pull", appname, str(uuid4()))
        self.appname = appname
        #self.requestor = requestor
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time

@pcc_set
class AcceptPushObj(object):
    name = primarykey(str)
    start_time = dimension(str)
    end_time = dimension(str)
    stop_logging = dimension(bool)

    def __init__(self, appname):
        self.name = "{0}_{1}_{2}".format("Accept Push", appname, str(uuid4()))
        self.appname = appname
        #self.requestor = requestor
        self.start_time = " "
        self.end_time = " "
        self.stop_logging = False

    def set_start_time(self):
        self.start_time = str(time.ctime())

    def set_end_time(self):
        self.end_time = str(time.ctime())

    def stop_logging(self):
        self.stop_logging = True

    def __str__(self):
        return self.name + " " + "start: " + self.start_time + " " + "end: " + self.end_time


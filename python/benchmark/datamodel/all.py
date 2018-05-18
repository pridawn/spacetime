'''
Created on Apr 19, 2016

@author: Rohan Achar
'''
import pkgutil
import importlib
import inspect
import os

from rtypes.pcc.triggers import TriggerProcedure

DATAMODEL_TYPES = []
DATAMODEL_TRIGGERS = []
def load_all_sets(reload_modules=False):
    global DATAMODEL_TYPES, DATAMODEL_TRIGGERS
    OLD_DATAMODEL_TYPES = DATAMODEL_TYPES
    OLD_DATAMODEL_TRIGGERS = DATAMODEL_TRIGGERS
    DATAMODEL_TYPES = []
    DATAMODEL_TRIGGERS = []

    module_list = []
    datamodel_list = []
    p = os.getcwd()
    for _, name, ispkg in pkgutil.iter_modules(['benchmark/datamodel']):
        if ispkg:
            try:
                mod = importlib.import_module('benchmark.datamodel.' + name)
                module_list.append(mod)
                if reload_modules:
                    reload(mod)
            except:
                print "Failed to load module benchmark.datamodel.%s" % name
                raise

    for module in module_list:
        for _, name, _ in pkgutil.iter_modules(module.__path__):
            #try:
            mod = importlib.import_module(module.__name__ + '.' + name)
            datamodel_list.append(mod)
            if reload_modules:
                reload(mod)
            #except:
            #    print "Failed to load module %s.%s" % (module.__name__, name)

    for module in datamodel_list:
        for name, cls in inspect.getmembers(module, inspect.isclass):
            if hasattr(cls, "__rtypes_metadata__"):
                DATAMODEL_TYPES.append(cls)
        for name, obj in inspect.getmembers(module):
            if isinstance(obj, TriggerProcedure):
                DATAMODEL_TRIGGERS.append(obj)
    DATAMODEL_TYPES = list(set(DATAMODEL_TYPES).difference(OLD_DATAMODEL_TYPES))
    DATAMODEL_TRIGGERS = list(
        set(DATAMODEL_TRIGGERS).difference(OLD_DATAMODEL_TRIGGERS))

if not DATAMODEL_TYPES:
    load_all_sets()

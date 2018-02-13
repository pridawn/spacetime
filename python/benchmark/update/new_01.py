'''
Created on Aug 25, 2016

@author: Arthur Valadares
'''
'''
Created on Aug 24, 2016

@author: arthurvaladares
'''
from datamodel.benchmark.datamodel import BaseSet
import random

BM_PRODUCER = [BaseSet]
BM_GETTER = []
BM_SETTER = []
BM_GETTERSETTER = []
BM_TRACKER = []
BM_DELETER =[]

BT_PRODUCER = []
BT_GETTER = []
BT_SETTER = []
BT_GETTERSETTER = []
BT_TRACKER = [BaseSet]
BT_DELETER =[]

builtin_dict = globals()['__builtins__']
builtin_dict['BM_PRODUCER'] = BM_PRODUCER
builtin_dict['BM_SETTER'] = BM_SETTER
builtin_dict['BM_GETTER'] = BM_GETTER
builtin_dict['BM_GETTERSETTER'] = BM_GETTERSETTER
builtin_dict['BM_TRACKER'] = BM_TRACKER
builtin_dict['BM_DELETER'] = BM_DELETER

builtin_dict['BT_PRODUCER'] = BT_PRODUCER
builtin_dict['BT_GETTER'] = BT_GETTER
builtin_dict['BT_SETTER'] = BT_SETTER
builtin_dict['BT_GETTERSETTER'] = BT_GETTERSETTER
builtin_dict['BT_TRACKER'] = BT_TRACKER
builtin_dict['BT_DELETER'] = BT_DELETER

def initialize(sim):
    print "Benchmark test for New"

def initialize_test(sim):
    pass

def update(sim):
    for i in xrange(sim.instances):
        sim.frame.add(BaseSet(i))
    print "[BENCHMARK STEP]: %s" % sim.curstep

def update_test(sim):
    print "[BENCHMARK TEST]: Step %s and BaseSet count %s" % (sim.curstep, len(sim.frame.get(BaseSet)))

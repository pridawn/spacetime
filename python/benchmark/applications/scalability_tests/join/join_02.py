'''
Created on Aug 24, 2016

@author: arthurvaladares
'''
from benchmark.datamodel.base_tests.datamodel import BaseSet, JoinAll

import random

BM_PRODUCER = [BaseSet]
BM_GETTER = []
BM_SETTER = []
BM_GETTERSETTER = []
BM_TRACKER = []
BM_DELETER = []

BT_PRODUCER = []
BT_GETTER = []
BT_SETTER = []
BT_GETTERSETTER = []
BT_TRACKER = [JoinAll]
BT_DELETER = []

def initialize(sim):
    print "Benchmark test for JoinAll"
    frame = sim.frame
    for i in xrange(sim.instances):
        frame.add(BaseSet(i))

def initialize_test(_):
    pass

def update(sim):
    print "[BENCHMARK STEP]: %s" % sim.curstep

def update_test(sim):
    print "[BENCHMARK TEST]: Step %s and JoinAll count %s" % (
        sim.curstep, len(sim.frame.get(JoinAll)))

'''
Created on Aug 25, 2016

@author: Arthur Valadares
'''
from benchmark.datamodel.base_tests.datamodel import BaseSet

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
BT_TRACKER = [BaseSet]
BT_DELETER = []

def initialize(_):
    print "Benchmark test for New"

def initialize_test(_):
    pass

def update(sim):
    print "Instances", sim.instances
    for i in xrange(sim.instances):
        sim.frame.add(BaseSet((2*sim.curstep) + i))
    print "[BENCHMARK NEW STEP]: %s" % sim.curstep

def update_test(sim):
    print "[BENCHMARK NEW TEST]: Step %s and BaseSet count %s" % (
        sim.curstep, len(sim.frame.get(BaseSet)))

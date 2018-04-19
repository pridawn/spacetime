'''
Created on Aug 24, 2016

@author: arthurvaladares
'''
from benchmark.datamodel.base_tests.datamodel import BaseSet, SubsetAll

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
BT_TRACKER = [SubsetAll]
BT_DELETER = []

def initialize(sim):
    print "Benchmark test for SubsetAll"
    frame = sim.frame
    for i in xrange(sim.instances):
        frame.add(BaseSet(i))

def initialize_test(_):
    pass

def update(sim):
    print "[BENCHMARK SUBSET2 STEP]: %s" % sim.curstep

def update_test(sim):
    print "[BENCHMARK SUBSET2 TEST]: Step %s and SubsetAll count %s" % (
        sim.curstep, len(sim.frame.get(SubsetAll)))

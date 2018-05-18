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
BM_DELETER = [BaseSet]

BT_PRODUCER = []
BT_GETTER = []
BT_SETTER = []
BT_GETTERSETTER = []
BT_TRACKER = [BaseSet]
BT_DELETER = []

def initialize(sim):
    print "Benchmark test for DEl"
    frame = sim.frame
    for i in xrange(2*sim.simsteps):
        frame.add(BaseSet(i))

def initialize_test(_):
    pass

def update(sim):
    print "Instances", sim.instances
    for i in xrange(sim.instances):
        sim.frame.delete(BaseSet, BaseSet((2*sim.curstep) + i))
    print "[BENCHMARK DEL STEP]: %s" % sim.curstep

def update_test(sim):
    print "[BENCHMARK DEL TEST]: Step %s and BaseSet count %s" % (
        sim.curstep, len(sim.frame.get(BaseSet)))

'''
Created on Aug 25, 2016

@author: Arthur Valadares
'''
from benchmark.datamodel.base_tests.datamodel import BaseSet

BM_PRODUCER = [BaseSet]
BM_GETTER = []
BM_SETTER = [BaseSet]
BM_GETTERSETTER = []
BM_TRACKER = []
BM_DELETER = []

BT_PRODUCER = []
BT_GETTER = [BaseSet]
BT_SETTER = []
BT_GETTERSETTER = []
BT_TRACKER = []
BT_DELETER = []

def initialize(sim):
    print "Benchmark test for Updates"
    frame = sim.frame
    for i in xrange(sim.instances):
        frame.add(BaseSet(i))

def initialize_test(_):
    pass

def update(sim):
    for obj in sim.frame.get(BaseSet):
        obj.Number += 1
    print "[BENCHMARK UPDATE STEP]: %s" % sim.curstep

def update_test(sim):
    print "[BENCHMARK UPDATE TEST]: Step %s and BaseSet Num sum = %s" % (
        sim.curstep, sum(o.Number for o in sim.frame.get(BaseSet)))

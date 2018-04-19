'''
Created on Aug 25, 2016

@author: Arthur Valadares
'''
from benchmark.datamodel.base_tests.datamodel import BaseSet

BT_PRODUCER = [BaseSet]
BT_GETTER = []
BT_SETTER = [BaseSet]
BT_GETTERSETTER = []
BT_TRACKER = []
BT_DELETER = []

BM_PRODUCER = []
BM_GETTER = [BaseSet]
BM_SETTER = []
BM_GETTERSETTER = []
BM_TRACKER = []
BM_DELETER = []

def initialize_test(sim):
    print "Benchmark test for Updates"
    frame = sim.frame
    for i in xrange(sim.instances):
        frame.add(BaseSet(i))

def initialize(_):
    pass

def update_test(sim):
    for obj in sim.frame.get(BaseSet):
        obj.Number += 1
    # print "[BENCHMARK UPDATE STEP]: %s" % sim.curstep

def update(sim):
    print "[BENCHMARK UPDATE TEST]: Step %s and BaseSet Num sum = %s" % (
        sim.curstep, sum(o.Number for o in sim.frame.get(BaseSet)))

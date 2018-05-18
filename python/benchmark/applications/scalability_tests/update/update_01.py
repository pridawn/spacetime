'''
Created on Aug 25, 2016

@author: Arthur Valadares
'''
import os, time
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

tsum = 0
start_valid, start_regular = 0, 0
def initialize(sim):
    print "Benchmark test for Updates"
    frame = sim.frame
    for i in xrange(sim.instances):
        frame.add(BaseSet(i))

def initialize_test(_):
    # global start_regular, start_valid
    # start_valid = time.time()
    # start_regular = start_valid
    pass

def update(sim):
    for obj in sim.frame.get(BaseSet):
        obj.Number += 1
    # print "[BENCHMARK UPDATE STEP]: %s" % sim.curstep

def update_test(sim):
    s = sum(o.Number for o in sim.frame.get(BaseSet))
    # if s != tsum:
    #     tsum = s
    #     if not os.path.exists(sim.foldername):
    #         os.makedirs(sim.foldername)
    #     open(os.path.join(
    #         sim.foldername, "{0}_data_valid_step.txt".format(sim.sim_name)),
    #             "a").write("{0}\n".format(sim.curstep))
    # print "[%s]: Step %s and BaseSet Num sum = %s" % (
    #     sim.sim_name, sim.curstep, s)

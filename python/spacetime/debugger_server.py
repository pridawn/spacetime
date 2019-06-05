from spacetime.dataframe import Dataframe
from threading import Thread
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime.debug_dataframe import DebugDataframe
from spacetime.debugger_types import CheckoutObj, CommitObj, PushObj, PullObj, AcceptPullObj, \
    AcceptPushObj, ConfirmPullObj, Register
import copy
import time


def register_func(df, appname):

    register_obj = Register(appname)
    df.add_one(Register, register_obj)
    df.commit()
    df.push()
    while register_obj.port is 0:
        # time.sleep(5)
        df.pull()
    return register_obj.port


def server_func(df):

    existing_obj ={"CheckoutObj" : [], "CommitObj" : [], "PushObj" : [], "PullObj" : [],"AcceptPullObj" : [], "AcceptPushObj" : [],
                   "ConfirmPullObj" : []}
    dataframes = dict()

    def check_for_new_nodes():
        no_of_nodes_joined = 0
        existing_register_objects = []
        while True:
            df.pull()
            new_register_objects = df.read_all(Register)
            for register_obj in new_register_objects:
                if register_obj not in existing_register_objects:
                    existing_register_objects.append(register_obj)

                    print("A new node registers with the server:" + str(register_obj.appname) + "\n")
                    f = open("debugger_log.txt", "a+")
                    f.write("A new node registers with the server:" + str(register_obj.appname) + "\n")
                    f.close()

                    current_df = Dataframe(register_obj.appname,
                                           {CheckoutObj, CommitObj, PushObj, PullObj, AcceptPullObj, AcceptPushObj,
                                            ConfirmPullObj})  # Create a dataframe for the new client
                    print("The dataframe that is created for this node is" + str(current_df) + "\n")
                    dataframes[register_obj.appname] = current_df
                    register_obj.port = current_df.details[1]

                    print("The port assigned to this node is " + str(register_obj.port) + "\n")
                    f = open("debugger_log.txt", "a+")
                    f.write("The port assigned to this node is " + str(register_obj.port) + "\n")
                    f.close()

                    no_of_nodes_joined += 1
                    print("no of nodes that have registered with the server is " + str(no_of_nodes_joined) + "\n")
                    f = open("debugger_log.txt", "a+")
                    f.write("no of nodes that have registered with the server is " + str(no_of_nodes_joined) + "\n")
                    f.close()

                    df.commit()

    def logger():
        while True:
            current_dataframes = dataframes.copy()
            for df in current_dataframes.values():
                df.pull()
                for type in existing_obj.keys():
                    new_objects = df.read_all(eval(type))
                    for obj in new_objects:
                        if obj not in existing_obj[type]:
                            existing_obj[type].append(obj)

                            print("I'm in debugger server", obj)
                            f = open("debugger_log.txt", "a+")
                            f.write("Operation: " + str(obj) + "\n")
                            f.close()
                        else:
                            if obj.end_time != " " and not obj.stop_logging:
                                obj.stop_logging()

                                print("I'm in debugger server", obj)
                                f = open("debugger_log.txt", "a+")
                                f.write("Operation: " + str(obj) + "\n")
                                f.close()

    f = open("debugger_log.txt", "w+")
    f.write("Logging:"+ "\n")
    f.close()
    check_for_new_nodes_thread = Thread(target=check_for_new_nodes)
    check_for_new_nodes_thread.start()
    logger_thread = Thread(target=logger)
    logger_thread.start()
    check_for_new_nodes_thread.join()
    logger_thread.join()
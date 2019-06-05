from spacetime.dataframe import Dataframe
from spacetime.debugger_types import CheckoutObj, CommitObj, PushObj, PullObj, AcceptPullObj,\
    AcceptPushObj, ConfirmPullObj


class DebugDataframe():

    def __init__(self, df, appname, types, server_port, parent_details):
        print(appname, types, server_port, parent_details)
        self.application_df = Dataframe(appname, types, details=parent_details, server_port=server_port)
        print("application dataframe details", self.application_df.details)
        self.debugger_df = df
        print("debugger dataframe details", self.debugger_df.details)

    def add_one(self, dtype, obj):
        self.application_df.add_one(dtype, obj)

    def add_many(self, dtype, objs):
        self.application_df.add_many(dtype.obj)

    def read_one(self, dtype, oid):
        print(self.application_df.read_one(dtype, oid))
        return self.application_df.read_one(dtype, oid)

    def read_all(self, dtype):
       return self.application_df.read_all(dtype)

    def delete_one(self, dtype, obj):
        self.application_df.delete_one(dtype, obj)

    def delete_all(self, dtype):
        self.application_df.delete_all(dtype)

    def checkout(self):
        checkoutObj = CheckoutObj(self.application_df.appname)

        checkoutObj.set_start_time()
        self.debugger_df.add_one(CheckoutObj, checkoutObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.checkout()

        checkoutObj.set_end_time()

        self.debugger_df.commit()
        self.debugger_df.push()

    def commit(self):
        commitObj = CommitObj(self.application_df.appname)


        commitObj.set_start_time()
        self.debugger_df.add_one(CommitObj, commitObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.commit()

        commitObj.set_end_time()
        self.debugger_df.commit()
        self.debugger_df.push()

    def sync(self):
         self.application_df.sync()

    def push(self):
        pushObj = PushObj(self.application_df.appname)

        pushObj.set_start_time()
        self.debugger_df.add_one(PushObj, pushObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.push()

        pushObj.set_end_time()
        self.debugger_df.commit()
        self.debugger_df.push()

    def pull(self):
        pullObj = PullObj(self.application_df.appname)

        pullObj.set_start_time()
        self.debugger_df.add_one(PullObj, pullObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.push()

        pullObj.set_end_time()
        self.debugger_df.commit()
        self.debugger_df.push()

    def pull_call_back(self, appname, version):
        acceptPullObj = AcceptPullObj(self.application_df.appname)

        acceptPullObj.set_start_time()
        self.debugger_df.add_one(AcceptPullObj, acceptPullObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.pull_call_back(appname, version)

        acceptPullObj.set_end_time()
        self.debugger_df.commit()
        self.debugger_df.push()

    def push_call_back(self, appname, versions, data):
        acceptPushObj = AcceptPushObj(self.application_df.appname)

        acceptPushObj.set_start_time()
        self.debugger_df.add_one(AcceptPushObj, acceptPushObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.push_call_back(appname, versions, data)

        acceptPushObj.set_end_time()
        self.debugger_df.commit()
        self.debugger_df.push()

    def confirm_pull_req(self, appname, version):
        confirmPullObj = ConfirmPullObj(self.application_df.appname)

        confirmPullObj.set_start_time()
        self.debugger_df.add_one(ConfirmPullObj, confirmPullObj)
        self.debugger_df.commit()
        self.debugger_df.push()

        self.application_df.confirm_pull_req(appname, version)

        confirmPullObj.set_end_time()
        self.debugger_df.commit()
        self.debugger_df.push()

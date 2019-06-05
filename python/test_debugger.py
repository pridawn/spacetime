from spacetime import Node, Dataframe
from rtypes import pcc_set, primarykey, dimension, merge
from spacetime.debug_node import Register, server_func


@pcc_set
class Foo:
    y = primarykey(int)

    def __init__(self, i):
        self.i = i


def client_func(df, i):
    df.add_one(Foo, Foo(i))
    df.commit()
    print(df.read_one(Foo, Foo(i)).i)


def main():
    debugger_server = Node(server_func, Types=[Register], server_port=30000)
    debugger_server.start_async()
    client = Node(client_func, Types=[Foo], debug=True)
    client.start_async(1)
    client.join()
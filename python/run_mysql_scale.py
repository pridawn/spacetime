from mysql.connector import MySQLConnection
from mysql.connector import errors
from multiprocessing import Event
from multiprocessing import Queue
from multiprocessing import Process as Parallel
from threading import Thread
import time
#from threading import Thread as Parallel


class BaseSet(object):
    @property
    def ID(self): return self._id
    @ID.setter
    def ID(self, v): self._id = v
    @property
    def NUMBER(self): return self._num
    @NUMBER.setter
    def NUMBER(self, v): self._num = v
    @property
    def prop1(self): return self._p1
    @prop1.setter
    def prop1(self, v): self._p1 = v

    @property
    def prop2(self): return self._p2
    @prop2.setter
    def prop2(self, v): self._p2 = v

    @property
    def prop3(self): return self._p3
    @prop3.setter
    def prop3(self, v): self._p3 = v

    @property
    def prop4(self): return self._p4
    @prop4.setter
    def prop4(self, v): self._p4 = v
    
    def __init__(self, oid, num, p1, p2, p3, p4):
        self.ID = oid
        self.NUMBER = num
        self.prop1 = p1
        self.prop2 = p2
        self.prop3 = p3
        self.prop4 = p4

    def serial(self):
        return self.ID, self.NUMBER, self.prop1, self.prop2, self.prop3, self.prop4

def pull_c():
    sql_con = get_con()
    cur = sql_con.cursor()
    cur.execute("SELECT * FROM BaseSet;")
    results = list()

    for result in cur.fetchall():
        results.append(BaseSet(*result))
    cur.close()
    sql_con.close()
    return results

def update_c(data):
    return sum([bs.NUMBER for bs in data])


def update_new(data):
    new = [BaseSet(len(data), len(data), "p1", "p2", "p3", "p4"),
           BaseSet(len(data)+1, len(data)+1, "p1", "p2", "p3", "p4")]
    data += new
    return new, [], []

def update_mod(data):
    for bs in data:
        bs.NUMBER += 1 
    return [], data, []

def update_del(data):
    if len(data) > 2:
        dele = [data[-2], data[-1]]
    data.remove(data[-2])
    data.remove(data[-1])
    return [], [], dele

def push_m(new, mod, delete):
    sql_con = get_con()
    sql_con.start_transaction()
    cur = sql_con.cursor()
    if new:
        for bs in new:
            cur.execute(
                "INSERT INTO BaseSet (ID, NUMBER, prop1, prop2, prop3, prop4) VALUES(%s, %s, %s, %s, %s, %s);", bs.serial())
    if mod:
        for bs in mod:
            cur.execute(
                "UPDATE BaseSet SET NUMBER = %s WHERE ID = %s;", (bs.NUMBER, bs.ID))
    if delete:
        for bs in delete:
            cur.execute(
                "DELETE FROM BaseSet WHERE ID = %s;", (bs.ID,))
    sql_con.commit()
    cur.close()
    sql_con.close()


def get_con():
    return MySQLConnection(
        user="root", password="tyl0n4pi",
        host="127.0.0.1", database="benchmarks")


def cleanup(data):
    sql_con = get_con()
    sql_con.start_transaction()
    cur = sql_con.cursor()
    try:
        cur.execute("DROP TABLE BaseSet;")
    except Exception:
        pass
    cur.execute("CREATE TABLE BaseSet (ID INT PRIMARY KEY, NUMBER INT, prop1 TEXT, prop2 TEXT, prop3 TEXT, prop4 TEXT);")
    for bs in data:
        cur.execute("INSERT INTO BaseSet (ID, NUMBER, prop1, prop2, prop3, prop4) VALUES(%s, %s, %s, %s, %s, %s);", bs.serial())
    sql_con.commit()
    cur.close()
    sql_con.close()

def master(event, func, data):
    cleanup(data)
    while not event.wait(timeout=0.5):
        # pull_m()
        new, mod, delete = func(data)
        push_m(new, mod, delete)

def client(i, queue):
    while True:
        start = time.time()
        data = pull_c()
        s = update_c(data)
        delta = time.time() - start
        queue.put((i, delta * 1000))
        if delta < 0.5:
            time.sleep(0.5 - delta)

def main(func, data):
    client_to_time = dict()
    event = Event()
    prod = Parallel(target=master, args=(event, func, data))
    prod.daemon = True
    prod.start()

    queue = Queue()
    avg = [0]
    def read_q():
        while True:
            stat = queue.get()
            appname, value =  stat
            client_to_time[appname] = value
            if client_to_time:
                avg[0] = float(sum(client_to_time.values())/ len(client_to_time))

    stat_thread = Thread(target=read_q)
    stat_thread.daemon = True
    stat_thread.start()
    a = 0
    i = 0
    with open("stats/mysql_scale.txt", "w") as mfile:
        while a < 1000:
            time.sleep(5)
            print "AVG:", a, i
            clie = Parallel(target=client, args=(i, queue))
            clie.daemon = True
            clie.start()

            i += 1
            a = avg[0]
            mfile.write("{0},{1}\n".format(a, i))
            
    event.set()
    prod.join()

main(update_mod, [BaseSet(i, i, "abcdefg"*10000, "p2", "p3", "p4") for i in range(1000)])

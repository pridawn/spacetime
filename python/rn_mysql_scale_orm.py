import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from multiprocessing import Event
from multiprocessing import Queue
from multiprocessing import Process as Parallel
from threading import Thread
import time
 
Base = declarative_base()
 
class BaseSet(Base):
    __tablename__ = 'BaseSet'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    ID = Column(Integer, primary_key=True)
    NUMBER = Column(Integer, nullable=False)
 
 
# Create an engine that stores data in the local directory's
# sqlalchemy_example.db file.
engine = create_engine('mysql://root:rohan123@127.0.0.1/benchmarks')
Base.metadata.bind = engine
# Create all tables in the engine. This is equivalent to "Create Table"
# statements in raw SQL.
Base.metadata.create_all(engine)
DBSession = sessionmaker(bind=engine)


#from threading import Thread as Parallel


def pull_c():
    session = DBSession()
    return session.query(BaseSet).all()

def update_c(data):
    return sum([obj.NUMBER for obj in data])


def update_mod(data):
    for i in range(len(data)):
        data[i].NUMBER += 1

def push_m(session):
    session.commit()

def cleanup(data):
    session = DBSession()
    for d in session.query(BaseSet).all():
        session.delete(d)
    session.commit()
    session.close()
    session = DBSession()
    for bs in data:
        session.add(bs)
    session.commit()
    session.close()

def master(event, func, data):
    cleanup(data)
    while not event.wait(timeout=0.5):
        # pull_m()
        session = DBSession()
        data = session.query(BaseSet).all()
        func(data)
        push_m(session)
        session.close()

def client(i, queue):
    while True:
        start = time.time()
        data = pull_c()
        s = update_c(data)
        delta = time.time() - start
        print "CLIENT", i, delta, s
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


main(update_mod, [BaseSet(ID=i, NUMBER=i) for i in range(1000)])

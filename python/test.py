import logging
import time
from pympler import asizeof, summary
from benchmark.datamodel.base_tests.datamodel import BaseSet, SubsetHalf, BaseSetProjection

from rtypes.dataframe.dataframe import dataframe
from rtypes.dataframe.dataframe_client import dataframe_client
from rtypes.dataframe.application_queue import ApplicationQueue


def main():
    df = dataframe()
    df.add_types([BaseSet, SubsetHalf, BaseSetProjection])
    app_q_producer = ApplicationQueue(
        "PRODUCER", list(), df)
    app_q_consumer = ApplicationQueue(
        "CONSUMER", [BaseSet], df)

    df_c_producer = dataframe_client()
    df_c_producer.add_types([BaseSet])
    df_c_producer.start_recording = True

    df_c_consumer = dataframe_client()
    df_c_consumer.add_types([BaseSet])
    df_c_consumer.start_recording = True

    base_objs = [BaseSet(i) for i in xrange(1000)]
    df_c_producer.extend(BaseSet, base_objs)
    changes = df_c_producer.get_record()
    df.apply_changes(changes, except_app="PRODUCER")

    changes_b = app_q_consumer.get_record()
    df_c_consumer.apply_changes(changes_b)

    tick = 0
    while True:
        try:
            for obj in base_objs:
                obj.Number += 1
            #sum1 = summary.summarize(df_base_objs[0])
            start = asizeof.asizeof(df)
            changes_producer = df_c_producer.get_record()
            df_c_producer.clear_record()
            df.apply_changes(changes_producer, except_app="PRODUCER")

            changes_apply = app_q_consumer.get_record()
            app_q_consumer.clear_record()

            df_c_consumer.apply_changes(changes_apply)
            df_c_consumer.clear_record()

            end = asizeof.asizeof(df)
            #sum2 = summary.summarize(df_base_objs[0])

            tick += 1
            print "Completed tick %d, size difference = %d, items_count=%d" % (
                tick, end - start, len(df_c_consumer.get(BaseSet)))

            # print [end_v[i] - start_v[i] for i in range(len(start_v))]
            #summary.print_(summary.get_diff(sum1, sum2))
            time.sleep(1)
        except KeyboardInterrupt:
            break

main()
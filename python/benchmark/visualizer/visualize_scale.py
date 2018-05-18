import argparse
import os

import matplotlib
import matplotlib.pyplot as plt

def create_dir(*paths):
    dirname = os.path.join(*paths)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return dirname

def parse_test_results(filename):
    # pylint: disable=E1101
    stats_dump = [
        int(l.strip().split(",")[0])
        for l in open(filename).read().split("\n") if l.strip()]
    # pylint: enable=E1101
    return stats_dump

def parse_mode_results(stats_folder):

    for testname in os.listdir(stats_folder):
        filename = os.path.join(stats_folder, testname)
        if testname.endswith("queue_size.txt"):
            return parse_test_results(filename)

def main(args):
    colors = ["y", "m", "c", "g", "r", "b"]
    graphs = dict()
    stats_to_mode = dict()
    for foldername in os.listdir(args.input_folder):
        stat_folder = os.path.join(args.input_folder, foldername)
        if os.path.isdir(stat_folder):
            modename = foldername.split(".")[-2]
            stats_to_mode.setdefault(
                modename, list()).append(parse_mode_results(
                    stat_folder))
    for modename, stats in stats_to_mode.iteritems():
        sort_stats = sorted(stats, key=lambda x:len(x), reverse=True)
        max_size = len(sort_stats[0])
        final_avg = list()
        for i in range(max_size):
            s = 0.0
            c = 0
            for stat in sort_stats:
                if i < len(stat):
                    s += stat[i]
                    c += 1
            avg = float(s) / c
            final_avg.append((avg, i+1))
        graphs[modename] = {"result": (final_avg, colors.pop())}
    write_graphs(
        graphs,
        args.output_folder)

def write_graphs(graphs, outfolder):
    create_dir(outfolder)
    graph_inv = dict()
    for mode, graph_data in graphs.iteritems():
        for testname, test_stats in graph_data.iteritems():
            stats, color = test_stats
            graph_inv.setdefault(
                testname, list()).append((stats, color, mode))

    for testname, data_chain in graph_inv.iteritems():
        fig = plt.figure()
        filename = "scale_{0}.png".format(testname)
        filepath = os.path.join(outfolder, filename)
        fig.suptitle("Scale Results for all Modes.")
        for stats, color, mode in data_chain:
            y, x = zip(*stats)
            plt.plot(x, y, color=color, label=mode)
        plt.ylabel("Avg client turnaround")
        plt.xlabel("Number of Applications")
        plt.legend()
        fig.savefig(filepath, bbox_inches="tight")
        plt.close()



def look_client_main(args):
    pass

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        "-i", "--input_folder", type=str, default="stats",
        help="The folder where the benchmarks to parse are found.")
    PARSER.add_argument(
        "-o", "--output_folder", type=str, default="results",
        help="The folder where the results are to be dumped.")

    matplotlib.rcParams.update({"font.size": 22})
    main(PARSER.parse_args())
    #look_client_main(PARSER.parse_args())

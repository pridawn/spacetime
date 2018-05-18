import argparse
import os

from collections import namedtuple

import json as json
import matplotlib
import matplotlib.pyplot as plt



RESULT_F = "results_all"
STAT_F = "stats_all"
mode_to_colors = dict()
colors = ["y", "m", "c", "g", "r", "b"]
def main():
    if not os.path.exists(RESULT_F):
        os.makedirs(RESULT_F)
    
    for btype in os.listdir(STAT_F):
        btype_f = os.path.join(STAT_F, btype)
        btype_f_write = os.path.join(RESULT_F, btype)
        generate(btype_f, btype_f_write, btype)


def generate(inp_f, out_f, statname):
    results = dict()
    for filename in os.listdir(inp_f):
        data = scrape(os.path.join(inp_f, filename))
        results.setdefault(get_mode(filename), list()).append(data)
    draw_results(results, out_f, statname)

def scrape(path):
    try:
        return [(float(line.strip().split(",")[1]) * 1000) for line in open(path, "r").read().strip().split("\n") if line.strip()]
    except Exception:
        print line, path
        raise

def get_mode(filename):
    return filename.split("_")[0]

def draw_results(results, out_f, statname):

    graphs = dict()
    for mode, stats in results.iteritems():
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
        color = colors.pop() if mode not in mode_to_colors else mode_to_colors[mode]
        mode_to_colors[mode] = color
        graphs[mode] = {
            "result": (final_avg, color)}
    write_graphs(graphs, out_f, statname)

def create_dir(*paths):
    dirname = os.path.join(*paths)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return dirname


def write_graphs(graphs, outfolder, statname):
    create_dir(outfolder)
    graph_inv = dict()
    for mode, graph_data in graphs.iteritems():
        for testname, test_stats in graph_data.iteritems():
            stats, color = test_stats
            graph_inv.setdefault(
                testname, list()).append((stats, color, mode))

    for testname, data_chain in graph_inv.iteritems():
        fig = plt.figure()
        filename = "{0}.png".format(statname)
        filepath = os.path.join(outfolder, filename)
        fig.suptitle("{0} Results for all Modes.".format(statname))
        for stats, color, mode in data_chain:
            y, x = zip(*stats)
            plt.plot(x, y, color=color, label=mode)
        plt.ylabel("Avg client turnaround")
        if "cale" in statname:
            plt.xlabel("Number of applications")
        else:
            plt.xlabel("Time step")
        plt.ylim(0,2000)
        plt.legend()
        fig.savefig(filepath, bbox_inches="tight")
        plt.close()

main()

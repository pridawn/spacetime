import argparse
import os

from collections import namedtuple

import ujson as json
import matplotlib
import matplotlib.pyplot as plt

MAX_NCOLS = 5

class SpacetimeGraph(object):
    def __init__(self, modename, title, xlabel, ylabel, points, color):
        self.title = title
        self.xlabel = xlabel
        self.ylabel = ylabel
        self.points = points
        self.modename = modename
        self.color = color

    def plot(self):
        plt.plot(self.points)
        plt.ylabel(self.ylabel)
        plt.xlabel(self.xlabel)
        plt.title(self.title)

def create_dir(*paths):
    dirname = os.path.join(*paths)
    if not os.path.exists(dirname):
        os.makedirs(dirname)
    return dirname

def write_to_file(output_folder, testname, statname, results, separate_figures):
    # if separate figures: write each result in results in a new file.
    # if not separate figures: write all results in same file.
    nrows = int(len(results) / MAX_NCOLS) + 1
    ncols = MAX_NCOLS if nrows > 1 else len(results)
    if not separate_figures:
        fig = plt.figure()
        filename = "results_{0}.png".format(statname)
        filepath = os.path.join(output_folder, filename)
        fig.suptitle("Results for all tests.")
        for i in range(len(results)):
            result = results[i]
            splt = plt.subplot(nrows, ncols, i + 1)
            splt.plot(result.points, color=result.color)
            plt.ylabel(result.ylabel)
            plt.xlabel(result.xlabel)
            splt.title = plt.title(result.title)
        fig.savefig(filepath, bbox_inches="tight")
        plt.close()
    else:
        for i in range(len(results)):
            result = results[i]
            filename = "{0}_{1}.png".format(testname, statname)
            filepath = os.path.join(output_folder, result.modename, filename)
            fig = plt.figure()
            fig.plot(result.points)
            fig.ylabel(result.ylabel)
            fig.xlabel(result.xlabel)
            fig.title = plt.title(result.title)
            fig.savefig(filepath, bbox_inches="tight")
            plt.close()


def write_all_tests_to_file(output_folder, statname, graphs, separate_figures):
    fig = plt.figure()
    fig.set_size_inches(48, 36)
    fig.suptitle("Results for all tests.")
    filename = "results_{0}.png".format(statname)
    filepath = os.path.join(output_folder, filename)
    test_keys = graphs.keys()
    nrows = int(len(test_keys) / MAX_NCOLS) + 1
    ncols = MAX_NCOLS if nrows > 1 else len(test_keys)

    for i, testname in enumerate(test_keys):
        test_graphs = graphs[testname]
        splt = plt.subplot(nrows, ncols, i+1)
        max_v = 0
        for result in test_graphs:
            max_v = max(result.points) if max(result.points) > max_v else max_v
            splt.plot(result.points, color=result.color, label=result.modename)
            plt.ylabel(result.ylabel)
            plt.xlabel(result.xlabel)
            splt.title = plt.title(result.title)
        splt.legend()
        if statname == "time":
            plt.ylim(0, 2000)

    fig.savefig(filepath, dpi=80, bbox_inches='tight')
    plt.close()

def write_graphs(graphs, output_folder, separate_figures, serparate_modes):
    # separate_figures == True: each test type gets its own graph.
    # separate_figures == False: all test types are in one graph
    #                            (not multiple files, subplots in same file)
    # separate_modes == True: each mode gets its own folder.
    # separate_modes == False: each mode gets its own color
    #                          in one graph per type.
    create_dir(output_folder)
    if serparate_modes:
        for mode, type_graphs in graphs.iteritems():
            dirname = create_dir(output_folder, mode)
            memory_graphs = dict()
            time_graphs = dict()

            for test, graphs in type_graphs.iteritems():
                if "memory" in graphs:
                    memory_graphs[test] = [graphs["memory"]]
                if "time" in graphs:
                    time_graphs[test] = [graphs["time"]]
            write_all_tests_to_file(
                dirname, "memory",
                memory_graphs, separate_figures)
            write_all_tests_to_file(
                dirname, "time",
                time_graphs, separate_figures)
    else:
        memory_graphs = dict()
        time_graphs = dict()

        for mode, type_graphs in graphs.iteritems():
            for test, graphs in type_graphs.iteritems():
                if "memory" in graphs:
                    memory_graphs.setdefault(
                        test, list()).append(graphs["memory"])
                if "time" in graphs:
                    time_graphs.setdefault(
                        test, list()).append(graphs["time"])
        write_all_tests_to_file(
            output_folder, "memory",
            memory_graphs, separate_figures)
        write_all_tests_to_file(
            output_folder, "time",
            time_graphs, separate_figures)

def parse_test_results(modename, testname, filename, mode, stats_node, only_avg, mode_color):
    stats_dump = json.load(open(filename))  # pylint: disable=E1101
    for part in stats_node.split("."):
        stats_dump = stats_dump[part]
    results = stats_dump["values"]
    graphs = dict()
    xlabel = "Time steps."
    if mode in set(["TIME", "ALL"]):
        points = [p * 1000 for _, p, _ in results]
        if only_avg:
            p_avg = sum(points) / len(points)
            points = [p_avg] * len(points)
        ylabel = "time in milliseconds"
        graphs["time"] = SpacetimeGraph(
            modename, testname, xlabel, ylabel, points, mode_color)
    if mode in set(["MEMORY", "ALL"]):
        points = [p / 1000000 for _, _, p in results]
        if only_avg:
            p_avg = sum(points) / len(points)
            points = [p_avg] * len(points)
        ylabel = "memory in MB"
        graphs["memory"] = SpacetimeGraph(
            modename, testname, xlabel, ylabel, points, mode_color)
    return graphs

def parse_mode_results(
        modename, stats_folder, mode,
        stats_client_node, stats_server_node, only_avg, mode_color):
    results = dict()
    for testname in os.listdir(stats_folder):
        filename = os.path.join(stats_folder, testname)
        if testname.startswith("Server_"):
            results[testname] = parse_test_results(
                modename, testname, filename, mode,
                stats_server_node, only_avg, mode_color)
        else:
            results[testname] = parse_test_results(
                modename, testname, filename, mode,
                stats_client_node, only_avg, mode_color)
    return results

def main(args):
    colors = ["y", "m", "c", "g", "r", "b"]
    graphs = dict()
    for foldername in os.listdir(args.input_folder):
        stat_folder = os.path.join(args.input_folder, foldername)
        if os.path.isdir(stat_folder):
            modename = foldername.split(".")[-2]
            graphs[modename] = parse_mode_results(
                modename, stat_folder, args.mode,
                args.stats_client_node, args.stats_server_node, args.only_avg,
                colors.pop())

    write_graphs(
        graphs,
        args.output_folder, args.separate_figures, args.separate_modes)


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        "-i", "--input_folder", type=str, default="stats",
        help="The folder where the benchmarks to parse are found.")
    PARSER.add_argument(
        "-o", "--output_folder", type=str, default="results",
        help="The folder where the results are to be dumped.")
    PARSER.add_argument(
        "-cn", "--stats_client_node", type=str, default="client.one_step",
        help="The states that have to be dumped out.")
    PARSER.add_argument(
        "-sn", "--stats_server_node", type=str,
        default="server.dataframe.process_apply_req",
        help="The states that have to be dumped out.")
    PARSER.add_argument(
        "-sf", "--separate_figures", action="store_true", default=False,
        help="Draw the results in multiple files, or just one file.")
    PARSER.add_argument(
        "-sm", "--separate_modes", action="store_true", default=False,
        help="Draw the results in multiple files, or just one file.")
    PARSER.add_argument(
        "-m", "--mode", type=str, default="ALL",
        help="Which results to draw. One of (ALL, MEMORY, TIME).")
    PARSER.add_argument(
        "-a", "--only_avg", action="store_true", default=False,
        help="Which results to draw. One of (ALL, MEMORY, TIME).")

    matplotlib.rcParams.update({"font.size": 22})
    main(PARSER.parse_args())

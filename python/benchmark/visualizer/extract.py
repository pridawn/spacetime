import argparse
import json

def parse_test_results(filename, stats_node):
    stats_dump = json.load(open(filename))  # pylint: disable=E1101
    for part in stats_node.split("."):
        stats_dump = stats_dump[part]
    return [p for _, p, _ in stats_dump["values"]]

def main(inp_f, out_f, cn):
    data = parse_test_results(inp_f, cn)
    open(out_f, "w").write("\n".join(["{0},{1}".format(i, data[i]) for i in range(len(data))]) + "\n")

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        "-i", "--input_file", type=str, default="stats",
        help="The folder where the benchmarks to parse are found.")
    PARSER.add_argument(
        "-o", "--output_file", type=str, default="results",
        help="The folder where the results are to be dumped.")
    PARSER.add_argument(
        "-cn", "--stats_client_node", type=str, default="client.one_step",
        help="The states that have to be dumped out.")
    args = PARSER.parse_args()
    main(args.input_file, args.output_file, args.stats_client_node)
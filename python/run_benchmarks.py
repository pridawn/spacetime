from __future__ import absolute_import
import importlib
import time
import os
import argparse
import logging

from benchmark.common.base_test_suite import BaseTestSuite, REGISTERED_TESTSUITES

def load_tests(testcases=None):
    import_mods = [
        "benchmark.applications.{0}.test".format(foldername)
        for foldername in (
            os.listdir("benchmark/applications")
            if not testcases else
            testcases)
        if os.path.isdir(
            os.path.join("benchmark/applications", foldername))]
    failed = list()
    modules = list()
    for mod_str in import_mods:
        try:
            modules.append(importlib.import_module(mod_str))
        except ImportError:
            failed.append(mod_str)

    if failed:
        print "Failed to load these modules. Stopping benchmarks."
        print "\n".join(failed)
        return list()
    return modules

def run_tests(args):
    for testcls in REGISTERED_TESTSUITES:
        name = "{0}.{1}".format(
            testcls.__module__, testcls.__name__)
        foldername = os.path.join(
            args.output_folder,
            "{0}.{1}".format(name, time.ctime().replace(" ", "-").replace(":", "_")))
        logger = setup_logger(name, args.logfile)
        test_suite = testcls(args, foldername, logger)
        test_suite.startup()
        test_suite.load_test_cases()
        test_suite.run()
        test_suite.cleanup()

def main(args):
    if load_tests(testcases=args.tests):
        run_tests(args)

def setup_logger(name, file_path=None):
    """
    Set up the loggers for this client frame.

    Arguments:
        name: Name of the client application.
        file_path: logfile to write logs into.

    Exceptions:
    None
    """

    logger = logging.getLogger(name)
    # Set default logging handler to avoid "No handler found" warnings.
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.DEBUG)
    if file_path:
        folder = os.path.dirname(file_path)
        if not os.path.exists(folder):
            os.makedirs(folder)
        flog = logging.handlers.RotatingFileHandler(
            file_path, maxBytes=10 * 1024 * 1024, backupCount=50, mode='w')
        flog.setLevel(logging.DEBUG)
        flog.setFormatter(
            logging.Formatter('%(levelname)s [%(name)s] %(message)s'))
        logger.addHandler(flog)

    logger.debug("Starting logger for %s", name)
    return logger


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument(
        "-i", "--instances", type=int, default=1000,
        help="Number of object instances to be instantiated.")
    PARSER.add_argument(
        "-s", "--steps", type=int, default=100,
        help="Number of simulation steps.")
    PARSER.add_argument(
        "-t", "--tests", type=str, default=list(), nargs="+",
        help="Name of tests to be run")
    PARSER.add_argument(
        "-a", "--address", default="http://127.0.0.1")
    PARSER.add_argument(
        "-p", "--port", default="12000")
    PARSER.add_argument(
        "-tf", "--testfile", default="benchmark/testlist.txt",
        help="File contanining list of tests in the form of "
        "<test_suite> <test_name> <instances> <steps> <testsims>.")
    PARSER.add_argument(
        "-tsp", "--timestep", type=int, default=500,
        help="Time interval for each simulation step. Default is 500.")
    PARSER.add_argument(
        "-m", "--mode", default=None,
        help="Testbench mode, current options are: "
        "<FULLNOWAIT>, <FULLWAIT>, <MYSQL>, <OBJLESSWAIT>, <OBJLESSNOWAIT>")
    PARSER.add_argument(
        "-sqlu", "--user", default=None,
        help="Username for MySQL if using MySQL connection")
    PARSER.add_argument(
        "-sqlp", "--password", default=None,
        help="Password for MySQL if using MySQL connection")
    PARSER.add_argument(
        "-sqldb", "--database", default=None,
        help="Database name for MySQL if using MySQL connection")
    PARSER.add_argument(
        "-dbg", "--debug", action="store_true", default=False,
        help="Database name for MySQL if using MySQL connection")
    PARSER.add_argument(
        "-o", "--output_folder", default="stats",
        help="Folder to put all the results into")
    PARSER.add_argument(
        "-lf", "--logfile", default=None,
        help="Log file to write logs to.")

    main(PARSER.parse_args())

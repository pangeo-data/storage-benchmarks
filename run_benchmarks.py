#!/usr/bin/env python

"""
    Script to kick off ASV tests then process JSON results into CSV format.
    Determines platform tests are being run on and will just process results
    for current run.

"""

from subprocess import call
import argparse
import fileinput
import glob
import os
import pandas as pd
import json
import re
import sys

ASV_MACHINE = os.path.expanduser('~/.asv-machine.json')
ASV_CONF    = 'asv.conf.json'
INIT_PY     = 'benchmarks/__init__.py'

class results_parser:
    """
    Generate formatted CSV entries from ASV output

    Attributes:
        asv_results: File that contains results from last run.
        machine_name: ASV user created name of platform.
        arch: Architecture of machine
        cpu: CPU arch.
        machine: ASV user generated description of platform/machine
        op_sys: Operating System architecture and version
        ram: Memory of machine in bytes.

    """
    def __init__(self, asv_results, machine_name, arch, cpu, machine, op_sys, ram):
        """ Initialize with outputted JSON results from ASV

            ToDo: may likely need to deal with benchmarks with different
                  configuration of parameters.

        """
        with open(asv_results) as results_f:
            self.machine_name = machine_name
            self.arch         = arch
            self.cpu          = cpu
            self.machine      = machine
            self.op_sys       = op_sys
            self.ram          = float(ram) / 2**30 # Convert to GB
            self.json_results = json.load(results_f)

        # Will truncate hash to first 8 chars as that should be enough and to
        # prevent crazy long entries
        self.commit_hash = self.json_results['commit_hash'][0:8]
        self.benchmarks = self.json_results['results']
        self.ds_sizes = {}
        self.results = []

        # Get sizes of datasets in benchmarks. Assumption is that test with
        # 'track' in string should be a dataset size.
        for benchmark in self.benchmarks:
            if re.search(r'track', benchmark):
                self.ds_sizes[benchmark] = self.benchmarks[benchmark]

        for benchmark in self.benchmarks:
            # Bypass dataset size results
            if not re.search(r'track', benchmark):
                # Assumption here is that params on test are consistently
                # ordered. Somewhat loosely:
                # 0 - Platform
                # 1 - chunk configuration
                # 2 - n_workers
                # 3 - run_num 
                platforms = self.json_results['results'][benchmark]['params'][0]
                z_chunks  = self.json_results['results'][benchmark]['params'][1]
                n_workers = self.json_results['results'][benchmark]['params'][2]
                run_nums  = self.json_results['results'][benchmark]['params'][3]
                results   = self.json_results['results'][benchmark]['result']

                # Now loop through each parameter to produce CSV output
                for i, platform in enumerate(platforms):
                    for j, z_chunk in enumerate(z_chunks):
                        for k, n_worker in enumerate(n_workers):
                            for l, run_num in enumerate(run_nums):
                                nth_run = i+j+k+l
                                result_str = ('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                                              '%s,%s' % (self.machine_name,
                                              self.arch, self.cpu,
                                              self.machine, self.op_sys,
                                              self.ram, benchmark, platform,
                                              z_chunk, n_worker, run_num,
                                              results[nth_run]))
                                self.results.append(result_str)

    def get_results(self):
        """Spot check raw JSON output. Might be useful for debugging"""
        return self.results

###############################################################################
#
# MAIN
#
###############################################################################


def get_env():
    """
        Determine required environmental variables for storage-benchmarks on
        particular machine.

        Returns:
            test_conf: By default test.conf.yaml but will parse 
                       benchmarks/__init__.py to determine.
            machine_name: How ASV recognizes machine benchmarks are being
                          run on.
            results_dir: Directory that contains JSON output ASV generates.

    """
    # Get machine name and determine directory where results will be
    #.a = ''
    arch         = ''
    cpu          = ''
    machine      = ''
    op_sys       = ''
    ram          = ''
    results_root = ''
    results_dir  = ''

    # Get machine name
    try:
        with open(ASV_MACHINE) as machine_f:
            machine_info = json.load(machine_f)
            machine_name = [*machine_info][0]
            arch         = machine_info[machine_name]['arch']
            cpu          = machine_info[machine_name]['cpu']
            machine      = machine_info[machine_name]['machine']
            op_sys       = machine_info[machine_name]['os']
            ram          = machine_info[machine_name]['ram']

    except IOError:
        print('%s not found or cannot be read.' %s (ASV_MACHINE_JSON))

    
    # Get results directory
    try:
        asv_conf_str = ''
        # Unfortunately, need to remove C-style comments from file
        # which are non-standard.
        pattern = re.compile(r"\/\/") # if we find a comment in file
        with open(ASV_CONF) as asv_conf_f:
            for line in asv_conf_f:
                if pattern.search(line):
                    continue
                else:
                    asv_conf_str += line
        asv_conf = json.loads(asv_conf_str)
        results_root = asv_conf["results_dir"]
    except IOError:
        print('%s not found or cannot be read.' %s (ASV_CONF))

    # Get test.conf.yaml file
    try:
        pattern = re.compile(r"^_CONFIG_FILE\s\=\s")
        with open(INIT_PY, 'rt') as open_file:
            for line in open_file:
                if pattern.search(line):
                    matched_line = line.split(' ')
                    # strip end spaces, then strip quotes
                    test_conf = matched_line[2].strip().strip("\"")
    except IOError:
        print('%s missing or cannot be read.' %s (INIT_PY))

    results_dir = results_root + '/' + machine_name
    
    return (test_conf, machine_name, arch, cpu,
            machine, op_sys, ram, results_dir)

def main():
    (test_conf, machine_name, arch, cpu,
     machine, op_sys, ram, results_dir) = get_env()

    # Handle commandline arguments and options to the application
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--benchmark", type=str, nargs="+",
                        dest="benchmark", required=True,
                        help="Regex of benchmarks to run.")
    parser.add_argument("-n", "--n_runs", type=int, nargs="+",
                        help="Number of benchmark runs.",
                        dest="n_runs")
    args = parser.parse_args()

    # Update test.conf.yaml file with number of runs we want
    if args.n_runs:
        pattern = re.compile(r"^n_runs:\s+\d+")
        try:
            open_file = open(test_conf, 'r')
            file_str  = open_file.read()
            open_file.close()
            file_str = re.sub(r'n_runs:\s+\d+', 'n_runs: %s' % args.n_runs[0],
                              file_str)
            # Write contents back to test.config.yaml
            open_file = open(test_conf, 'w')
            open_file.write(file_str)
            open_file.close()
        except IOError:
            print('%s not found' % test_conf)


    # Run ASV benchmarks according to user input.
    call(['asv', 'run', '-e', '-b', args.benchmark[0]])

    # Now, figure out where ASV has written results to parse and output to CSV.
    files = glob.glob(results_dir + '/*')
    results_file = max(files, key=os.path.getctime)   
    results = results_parser(results_file, machine_name,
                             arch, cpu, machine, op_sys, ram)
    csv_output = results.get_results()
 
    for item in csv_output:
        print(item)

if __name__ == '__main__':
    main()
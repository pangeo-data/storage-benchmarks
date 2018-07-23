#!/usr/bin/env python

"""
    Script to process results generated from ASC storage benchmark tests. Script 
    will scan through results directory and add entries into CSV file. Reasonable 
    attempts will be made to not duplicate entries through git commit hashes 
    present as prefixes in results JSON files, but shenanigans may not be well 
    tolerated: amending commits, etc. It is recommended that a separate results
    branch be kept in the repository.


"""

import os
import pandas
import json
import re

results_dir = 'results/kubernetes-gcp'



class results_parser:
    """
    Generate formatted CSV entries from ASV output

    Attributes:


    """
    def __init__(self, asv_results):
        """ Initialize with outputted JSON results from ASV

        """
        with open(asv_results) as results_f:
            self.json_results = json.load(results_f)

        # Will truncate hash to first 8 chars as that should be enough and to prevent
        # crazy long entries
        self.commit_hash = self.json_results['commit_hash'][0:8]
        self.benchmarks = self.json_results['results']
        self.ds_sizes = {}

        # Get sizes of datasets in benchmarks. Assumption is that test with 'track' in
        # string should be a dataset size.
        for benchmark in self.benchmarks:
            if re.search(r'track', benchmark):
                self.ds_sizes[benchmark] = self.benchmarks[benchmark]

        for benchmark in self.benchmarks:
            # Bypass dataset size results
            if not re.search(r'track', benchmark):
                # Assumption here is that params on test are consistently ordered.
                # Somewhat loosely: 
                # 0 - Platform
                # 1 - chunk configuration
                # 2 - n_workers
                print(benchmark)
                print(self.json_results['results'][benchmark])
        
                

    def return_json(self):
        """Spot check raw JSON output. Might be useful for debugging"""
        return self.json_results


def main():
    file_name = '45c0ba24-conda-py3.6-dask-distributed-gcsfs-h5netcdf-h5py-lz4-netcdf4-numpy-pip+dask_kubernetes-pip+h5pyd-pyyaml-rasterio-xarray-zarr.json'
    file_path = os.path.join(results_dir, file_name)
    test_result = results_parser(file_path)



if __name__ == '__main__':
    main()

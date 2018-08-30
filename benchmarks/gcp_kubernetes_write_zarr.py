"""
    Kubernetes/Dask Zarr write performance

"""

from . import target_zarr
from . import benchmark_tools as bmt
from . import getTestConfigValue
from dask.distributed import Client
from dask_kubernetes import KubeCluster
import dask.array as da
import itertools
import numpy as np
import timeit
import xarray as xr
import zarr

from os.path import abspath, dirname, join
from subprocess import call, Popen
from time import sleep
from pathlib import Path
import os
import tempfile
import itertools
import shutil
import timeit
import zarr
import tempfile

RETRIES = 5
RUNS = getTestConfigValue('n_runs')
DS_DIM = (3000, 3000, 3000)

class synthetic_benchmarks():
    """Zarr GCP tests on random synthetic Datasets

    """
    timer = timeit.default_timer
    timeout = 600
    repeat = 1
    number = 1
    warmup_time = 0.0
    run_nums = np.arange(1, RUNS + 1)
    params = (['GCS'], [1], np.arange(60, 180, 20), run_nums)
    param_names = ['backend', 'z_chunksize', 'n_workers', 'run_num']

    @bmt.test_gcp
    def setup(self, backend, z_chunksize, n_workers, run_num):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        bmt.cluster_wait(self.client, n_workers)
        self.chunks = (3000, 3000, z_chunksize) 
        self.da = da.random.normal(10, 0.1, size=DS_DIM, chunks=self.chunks)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True,
                                            chunksize=self.chunks,
                                            shape=self.da.shape,
                                            dtype=self.da.dtype)
        self.target.get_temp_filepath()

    @bmt.test_gcp
    def time_synthetic_write(self, backend, z_chunksize, n_workers, run_num):
        self.da.store(self.target.storage_obj, lock=False)
    
    @bmt.test_gcp
    def teardown(self, backend, z_chunksize, n_workers, run_num):
        self.cluster.close()
        del self.da
        self.cluster.close()
        self.target.rm_objects()


class rand_ds_size():
    number = 1
    repeat = 1
    warmup_time = 0.0

    def track_megabytes(self):
        # HACK make it cleaner later
        chunks = DS_DIM
        size = DS_DIM
        dask_arr = da.random.normal(10, 0.1, size=size, chunks=chunks)
        return dask_arr.nbytes / 2**20

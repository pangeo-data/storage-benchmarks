
from . import target_zarr
from . import benchmark_tools as bmt
from . import getTestConfigValue
from subprocess import call
import os

import numpy as np
import xarray as xr

RUNS = getTestConfigValue('n_runs')

class synthetic_benchmarks():
    """
        A stub class and benchmark for now. Will flesh this out on later
        release.
        
    """
    timeout = 600
    number = 1
    repeat = 1
    warmup_time = 0.0
    run_nums = np.arange(1, RUNS + 1)
    params = (['POSIX'], [1], [1], run_nums)
    param_names = ['backend', 'z_chunksize', 'n_workers', 'run_num']

    def setup(self, backend, z_chunksize, n_workers, run_num):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()
        bmt.rand_xarray().to_zarr(self.target.storage_obj)

    def time_read(self, backend, z_chunksize, n_workers, run_num):
        ds = xr.open_zarr(self.target.storage_obj).load()

    def teardown(self, backend, z_chunksize, n_workers, run_num):
        self.target.rm_objects()


class synthetic_ds_size():
    number = 1
    timeout = 300
    repeat = 1
    warmup_time = 0.0

    def track_megabytes(self):
        target = target_zarr.ZarrStore(backend='POSIX')
        target.get_temp_filepath()
        bmt.rand_xarray().to_zarr(target.storage_obj)
        ds = xr.open_zarr(target.storage_obj)
        return ds.nbytes / 2**20 
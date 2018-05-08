'''
    Tests using Xarray datasets.

'''

from . import target_zarr, target_hdf5
from . import benchmark_tools as bmt
from subprocess import call
import os

import numpy as np
import xarray as xr
import gcsfs


class Read_Zarr():
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()
        bmt.rand_xarray().to_zarr(self.target.storage_obj)

    def time_synthetic_read(self, backend, n_chunks, n_workers):
        ds = xr.open_zarr(self.target.storage_obj).load()

    def time_synthetic_mean(self, backend, n_chunks, n_workers):
        ds = xr.open_zarr(self.target.storage_obj).load()
        ds.mean()

    def teardown(self, backend, n_chunks, n_workers):
        self.target.rm_objects()


class Write_Zarr():
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()
        self.ds = bmt.rand_xarray()

    def time_synthetic_write(self, backend, n_chunks, n_workers):
        self.ds.to_zarr(self.target.storage_obj)

    def teardown(self, backend, n_chunks, n_workers):
        self.target.rm_objects()


class Report_dataset_sizes():
    number = 1
    repeat = 1
    warmup_time = 0.0
    params = (['ALL'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.ds = bmt.rand_xarray()

    def track_ds_size(self, backend, n_chunks, n_workers):
        return self.ds.nbytes / 1024**2
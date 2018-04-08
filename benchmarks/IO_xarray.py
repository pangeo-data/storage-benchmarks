'''
    Tests using Xarray datasets.

'''

from . import target_zarr, target_hdf5
from . import benchmark_tools as bmt
from subprocess import call
import os

import numpy as np
import xarray as xr


class IORead_Zarr():
    timeout = 300
    number = 1
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'], [1, 5])
    param_names = ['backend', 'nt']

    def setup(self, backend, nt):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()

        if backend == 'GCS':
            gsutil_arg = "gs://%s" % self.target.gcs_zarr
            call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])

        bmt.rand_xarray(nt=nt).to_zarr(self.target.storage_obj)

    def time_synthetic_read(self, backend, nt):
        ds = xr.open_zarr(self.target.storage_obj).load()

    def teardown(self, backend, nt):
        self.target.rm_objects()

class IOWrite_Zarr():
    timeout = 300
    number = 1
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'], [1, 5])
    param_names = ['backend', 'nt']

    def setup(self, backend, nt):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()

        if backend == 'GCS':
            gsutil_arg = "gs://%s" % self.target.gcs_zarr
            call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])

    def time_synthetic_write(self, backend, nt):
        bmt.rand_xarray(nt=nt).to_zarr(self.target.storage_obj)

    def teardown(self, backend, nt):
        self.target.rm_objects()


# class Compute_Zarr_POSIXLocal(target_zarr.ZarrStore):

#     def setup(self):
#         self.create_objects(dset='xarray')
#         self.config_store(backend='POSIX')
#         self.ds.to_zarr(self.path)

#     def time_computemean(self):
#         xr.open_zarr(self.path).mean

#     def teardown(self):
#         self.rm_objects(backend='POSIX')


# class Compute_Random_ZarrGCS(target_zarr.ZarrStore):
#     number      = 1
#     warmup_time = 0.0
#     timeout = 300.0

#     def setup(self):
#         self.create_objects()
#         self.config_store(backend='GCS')
#         self.ds.to_zarr(self.gcszarr_bucket)

#     def time_computemean(self):
#         xr.open_zarr(self.gcszarr_bucket).mean

#     def teardown(self):
#         self.rm_objects(backend='GCS')
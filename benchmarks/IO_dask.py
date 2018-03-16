'''
    IO Tests on Dask performance

'''

from . import target_zarr, target_hdf5
import os
import tempfile
import itertools
import shutil
import numpy as np
import dask.array as da
import h5py
import zarr

_counter = itertools.count()
_DATASET_NAME = "default"


class IORead_zarr_POSIX_local(target_zarr.ZarrStore):
    def setup(self):
        return

    def time_writetest(self):
        return

    def time_fancywritecalculation(self):
        return


class IOWrite_zarr_POSIX_local(target_zarr.ZarrStore):
    def setup(self):
        return

    def time_writetest(self):
        return

    def time_fancywritecalculation(self):
        return


# class ComputeSum_zarr_POSIX_local(target_zarr.ZarrStore):
#     # chunks
#     params = [256, 64]
#     param_names = ['chunksize']

#     def setup(self, chunksize):
#         self.config_store(empty=False, backend='POSIX')
#         self.make_ds()
#         self.ds = zarr.open_array(self.path, mode='r')
#         self.da = da.from_array(self.ds, chunks=chunksize)

#     def time_sum(self, chunksize):
#         return self.da.sum().compute()

#     def teardown(self, chunksize):
#         self.rm_objects()


class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        return

    def time_readtest(self):
        return

    def teardown(self):
        return

class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        return

    def time_writetest(self):
        return

    def teardown(self):
        return

class ComputeSum_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    # chunks
    params = [256, 64]
    param_names = ['chunksize']

    def setup(self, chunksize):
        self.create_objects(empty=False)
        self.f = h5py.File(self.path)
        self.ds = self.f[_DATASET_NAME]
        self.da = da.from_array(self.ds, chunks=chunksize)

    def time_sum(self, chunksize):
        return self.da.sum().compute()

    def teardown(self, chunksize):
        self.f.close()
        self.rm_objects()

"""
    Raw read/write performance of various backends and infrastructure
    TODO: should IO_raw just be combined with IO_numpy?


"""
from . import target_zarr, target_hdf5, target_hsds
from subprocess import call
import os
import tempfile
import itertools
import shutil
import numpy as np
import dask.array as da
import xarray as xr
import h5py
import zarr

_counter = itertools.count()
_DATASET_NAME = "default"

class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        return

    def time_readtest(self):
        return

    def time_fancycalculation(self):
        return


class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.create_objects()
        self.nz = 1024
        self.ny = 256
        self.nx = 512
        self.shape = (self.nz, self.ny, self.nx)
        self.dtype = 'f8'
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        self.h5file.create_dataset(_DATASET_NAME, data=self.data)
        # are these both necessary
        self.h5file.flush()
        self.h5file.close()

    def teardown_files(self):
        self.rm_objects()

class IOWrite_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        self.create_objects()
        self.nz = 1024
        self.ny = 256
        self.nx = 512
        self.shape = (self.nz, self.ny, self.nx)
        self.dtype = 'f8'
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        self.h5file.create_dataset(_DATASET_NAME, data=self.data)
        # are these both necessary
        self.h5file.flush()
        self.h5file.close()

    def teardown_files(self):
        self.rm_objects()

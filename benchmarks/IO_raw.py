"""
    Raw read/write performance of various backends and infrastructure
    TODO: should IO_raw just be combined with IO_numpy?


"""
from . import target_zarr, target_hdf5, target_hsds, getTestConfigValue
from subprocess import call
import os
import tempfile
import itertools
import shutil
import numpy as np
import dask.array as da
import xarray as xr
import h5py
import h5pyd
import zarr

_counter = itertools.count()

class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.nz = getTestConfigValue("num_slices")
        if not self.nz or self.nz <= 0: 
            raise NotImplementedError("num_slices invalid")
        self.create_objects(empty=False)

    def time_readtest(self):
        with h5py.File(self.path, 'r') as f:
            dset = f[self.dset_name]
            for i in range(self.nz):
                arr = dset[i,:,:]
                mean = arr.mean()
                if mean < 0.4 or mean > 0.6:
                    msg = "mean of {} for slice: {} is unexpected".format(mean, i)
                    raise ValueError(msg)

    def time_fancycalculation(self):
        return

    def teardown(self):
        self.rm_objects()


class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.create_objects()
        self.nz = getTestConfigValue("num_slices")
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        with h5py.File(self.path, 'a') as f:
            dset = f[self.dset_name]
            for i in range(self.nz):
                dset[i,:,:] = self.data[i,:,:]

    def teardown_files(self):
        self.rm_objects()

class IORead_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        if not self.username or not self.password or not self.endpoint:
            raise NotImplementedError("Missing config for HSDS tests")
        self.nz = getTestConfigValue("num_slices")
        self.create_objects(empty=False)

    def time_readtest(self):
        with h5pyd.File(self.path, 'r') as f:
            dset = f[self.dset_name]
            for i in range(self.nz):
                arr = dset[i,:,:]
                mean = arr.mean()
                if mean < 0.4 or mean > 0.6:
                    msg = "mean of {} for slice: {} is unexpected".format(mean, i)
                    raise ValueError(msg)

    def time_fancycalculation(self):
        return

    def teardown(self):
        self.rm_objects()

class IOWrite_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        if not self.username or not self.password or not self.endpoint:
            raise NotImplementedError("Missing config for HSDS tests")
        self.nz = getTestConfigValue("num_slices")
        self.ny = 256
        self.nx = 512
        self.shape = (self.nz, self.ny, self.nx)
        self.dtype = 'f8'
        self.create_objects()
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        # Writing the entire 1GB in one h5pyd call is not yet supported, so write in slices
        with h5pyd.File(self.path, 'a') as f:
            dset = f[self.dset_name]
            for i in range(self.nz):
                dset[i, :, :] = self.data[i, :, :]
         
    def teardown_files(self):
        self.rm_objects()

"""Numpy IO performance


"""

from . import target_hdf5, target_hsds, target_zarr, getTestConfigValue
from . import benchmark_tools as bmt

from subprocess import call
import random
import itertools
import numpy as np
import os

_counter = itertools.count()
_DATASET_NAME = "default"


def readtest(f):
    """Read all values of dataset and confirm they are in the expected range

    """
    dset = f[_DATASET_NAME]
    nz = dset.shape[0]
    for i in range(nz):
        arr = dset[i,:,:]
        mean = arr.mean()
        if mean < 0.4 or mean > 0.6:
            msg = "mean of {} for slice: {} is unexpected".format(mean, i)
            raise ValueError(msg)


def writetest(f, data):
    """Update all values of the dataset

    """
    dset = f[_DATASET_NAME]
    nz = dset.shape[0]
    for i in range(nz):
        dset[i,:,:] = data[i,:,:]

def tasmax_slicetest(f):
    """ Check random slice of tasmax dataset
    
    """
    dset = f['tasmax']
    day = random.randrange(dset.shape[0])  # choose random day in year
    data = dset[day,:,:]  # get numpy array for given day
    vals = data[np.where(data<400.0)]  # cull fill values
    min = vals.min()
    if min < 100.0:
        msg = "day: {} was unusually cold! (for degrees kelvin)".format(day)
        raise ValueError(msg)
    max = vals.max()
    if max > 350.0:
        msg = "day: {} was unusually hot! (for degrees kelvin)".format(day)
        raise ValueError(msg)
    if max - min < 20.0:
        msg = "day: {} expected more variation".format(day)
        raise ValueError(msg)


class Read_Random_Zarr():
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'],  [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()

        if backend == 'GCS':
            gsutil_arg = "gs://%s" % self.target.gcs_zarr
            call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])

        f = self.target.open(self.target.storage_obj, 'w')
        bmt.rand_numpy(f, empty=False)

    def time_readtest(self, backend, n_chunks, n_workers):
        f = self.target.open(self.target.storage_obj, 'r')
        readtest(f)

    def teardown(self, backend, n_chunks, n_workers):
        self.target.rm_objects()


class Write_Random_Zarr():
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.target = target_zarr.ZarrStore(backend=backend)
        self.target.get_temp_filepath()

        if backend == 'GCS':
             gsutil_arg = "gs://%s" % self.target.gcs_zarr
             call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])

        f = self.target.open(self.target.storage_obj, 'w')
        bmt.rand_numpy(f, empty=True)
        dset = f[_DATASET_NAME]
        self.dtype = dset.dtype
        self.shape = dset.shape
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self, backend, n_chunks, n_workers):
        f = self.target.open(self.target.storage_obj, 'a')
        writetest(f, self.data)

    def teardown(self, backend, n_chunks, n_workers):
        self.target.rm_objects()


class Read_Random_HDF5_POSIX(target_hdf5.SingleHDF5POSIXFile):
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX', 'FUSE'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.target = target_hdf5.SingleHDF5POSIXFile(backend=backend)
        self.path = self.get_temp_filepath()
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=False)
        f.close()

    def time_readtest(self, backend, n_chunks, n_workers):
        f = self.open(self.path, 'r')
        readtest(f)
        f.close()

    def teardown(self, backend, n_chunks, n_workers):
        self.rm_objects()


class Read_Random_HSDS(target_hsds.SingleHDF5HSDSFile):
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.path = self.get_temp_filepath()
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=False)
        f.close()

    def time_readtest(self, backend, n_chunks, n_workers):
        f = self.open(self.path, 'r')
        readtest(f)
        f.close()

    def teardown(self, backend, n_chunks, n_workers):
        self.rm_objects()


class Write_Random_HDF5_POSIX(target_hdf5.SingleHDF5POSIXFile):
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['POSIX', 'FUSE'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.target = target_hdf5.SingleHDF5POSIXFile(backend=backend)
        self.path = self.target.get_temp_filepath()    
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=True)
        dset = f[_DATASET_NAME]
        self.dtype = dset.dtype
        self.shape = dset.shape
        f.close()
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self, backend, n_chunks, n_workers):
        f = self.open(self.path, 'a')
        writetest(f, self.data)
        f.close()

    def teardown_files(self, backend, n_chunks, n_workers):
        self.rm_objects()


class Write_Random_HSDS(target_hsds.SingleHDF5HSDSFile):
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['HSDS'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.path = self.get_temp_filepath()
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=True)
        dset = f[_DATASET_NAME]
        self.dtype = dset.dtype
        self.shape = dset.shape
        f.close()
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self, backend, n_chunks, n_workers):
        f = self.open(self.path, 'a')
        writetest(f, self.data)
        f.close()

    def teardown_files(self, backend, n_chunks, n_workers):
        self.rm_objects()


class Select_LOCA_HSDS(target_hsds.SingleHDF5HSDSFile):
    timeout = 600
    number = 5
    repeat = 5
    warmup_time = 0.0
    params = (['HSDS'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.year = getTestConfigValue("loca_year_start")
        self.filepath = self.get_tasmax_filepath(year=self.year)

    def time_readslice(self, backend, n_chunks, n_workers):
        f = self.open(self.filepath, 'r')
        tasmax_slicetest(f)
        f.close()

    def teardown(self, backend, n_chunks, n_workers):
        pass


class Report_dataset_size():
    number = 1
    repeat = 1
    warmup_time = 0.0
    params = (['POSIX'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.nx = getTestConfigValue("np_nx")
        self.ny = getTestConfigValue("np_ny")
        self.nz = getTestConfigValue("np_nz")
        self.dtype = getTestConfigValue("np_dtype")
        self.ds = np.random.rand(*(self.nx,self.ny,self.nz)).astype(self.dtype)

    def track_ds_size(self, backend, n_chunks, n_workers):
        return self.ds.nbytes / 1024**2
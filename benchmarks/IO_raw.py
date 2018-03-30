"""
    Raw read/write performance of various backends and infrastructure
    TODO: should IO_raw just be combined with IO_numpy?


"""
from . import target_hdf5, target_hsds, getTestConfigValue
from . import benchmark_tools as bmt
import random
import itertools
import numpy as np

_counter = itertools.count()

_DATASET_NAME = "default"

# Read all values of dataset and confirm they are in the expected range
def readtest(f):
    dset = f[_DATASET_NAME]
    nz = dset.shape[0]
    for i in range(nz):
        arr = dset[i,:,:]
        mean = arr.mean()
        if mean < 0.4 or mean > 0.6:
            msg = "mean of {} for slice: {} is unexpected".format(mean, i)
            raise ValueError(msg)

# Update all values of the dataset
def writetest(f, data):
    dset = f[_DATASET_NAME]
    nz = dset.shape[0]
    for i in range(nz):
        dset[i,:,:] = data[i,:,:]

# Check random slice of tasmax dataset
def tasmax_slicetest(f):
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

class IORead_Random_POSIX(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.path = self.get_temp_filepath()
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=False)
        f.close()

    def time_readtest(self):
        f = self.open(self.path, 'r')
        readtest(f)
        f.close()
     
    def teardown(self):
        self.rm_objects()

class IORead_Random_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        self.path = self.get_temp_filepath()
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=False)
        f.close()

    def time_readtest(self):
        f = self.open(self.path, 'r')
        readtest(f)
        f.close()

    def teardown(self):
        self.rm_objects()


class IOWrite_Random_POSIX(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.path = self.get_temp_filepath()
        
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=True)
        dset = f[_DATASET_NAME]
        self.dtype = dset.dtype
        self.shape = dset.shape
        f.close()
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        f = self.open(self.path, 'a')
        writetest(f, self.data)
        f.close()

    def teardown_files(self):
        self.rm_objects()

class IOWrite_Random_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        self.path = self.get_temp_filepath()
        f = self.open(self.path, 'w')
        bmt.rand_numpy(f, empty=True)
        dset = f[_DATASET_NAME]
        self.dtype = dset.dtype
        self.shape = dset.shape
        f.close()
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        f = self.open(self.path, 'a')
        writetest(f, self.data)
        f.close()
         
    def teardown_files(self):
        self.rm_objects()

class IOSelect_LOCA_POSIX(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.year = getTestConfigValue("loca_year_start")
        self.filepath = self.get_tasmax_filepath(year=self.year)
        
    def time_readslice(self):
        f = self.open(self.filepath, 'r')
        tasmax_slicetest(f)
        f.close()

    def teardown(self):
        pass

class IOSelect_LOCA_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        self.year = getTestConfigValue("loca_year_start")
        self.filepath = self.get_tasmax_filepath(year=self.year)
        
    def time_readslice(self):
        f = self.open(self.filepath, 'r')
        tasmax_slicetest(f)
        f.close()

    def teardown(self):
        pass
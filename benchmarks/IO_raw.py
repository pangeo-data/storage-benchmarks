"""
    Raw read/write performance of various backends and infrastructure
    TODO: should IO_raw just be combined with IO_numpy?


"""
from . import target_hdf5, target_hsds, getTestConfigValue
import random
import itertools
import numpy as np

_counter = itertools.count()

<<<<<<< HEAD
# class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
#     def setup(self):
#         self.nz = getTestConfigValue("num_slices")
#         if not self.nz or self.nz <= 0: 
#             raise NotImplementedError("num_slices invalid")
#         self.create_objects(empty=False)

#     def time_readtest(self):
#         with h5py.File(self.path, 'r') as f:
#             dset = f[self.dset_name]
#             for i in range(self.nz):
#                 arr = dset[i,:,:]
#                 mean = arr.mean()
#                 if mean < 0.4 or mean > 0.6:
#                     msg = "mean of {} for slice: {} is unexpected".format(mean, i)
#                     raise ValueError(msg)

#     def time_fancycalculation(self):
#         return

#     def teardown(self):
#         self.rm_objects()


# class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
#     def setup(self):
#         self.create_objects()
#         self.nz = getTestConfigValue("num_slices")
#         self.data = np.random.rand(*self.shape).astype(self.dtype)

#     def time_writetest(self):
#         with h5py.File(self.path, 'a') as f:
#             dset = f[self.dset_name]
#             for i in range(self.nz):
#                 dset[i,:,:] = self.data[i,:,:]

#     def teardown_files(self):
#         self.rm_objects()
=======
_DATASET_NAME = "default"

# Use given handle to create a dataset
def create_objects(f, empty=True):
    nz = getTestConfigValue("num_slices")
    if not nz or nz <= 0: 
        raise NotImplementedError("num_slices invalid")
    ny = 256
    nx = 512
    dtype = 'f8'
    # Create a dataset
    dset = f.create_dataset(_DATASET_NAME, (nz,ny,nx), dtype=dtype)

    if not empty:
        # fill in some random data
        data = np.random.rand(*dset.shape).astype(dset.dtype)
        for i in range(nz):
            dset[i, :, :] = data[i, :, :]


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
        create_objects(f, empty=False)
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
        create_objects(f, empty=False)
        f.close()

    def time_readtest(self):
        f = self.open(self.path, 'r')
        readtest(f)
        f.close()

    def teardown(self):
        self.rm_objects()
>>>>>>> a5e63b24390f34f6b5f84f084e93102143eb79be

class IOWrite_Random_POSIX(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.path = self.get_temp_filepath()
        
        f = self.open(self.path, 'w')
        create_objects(f, empty=True)
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
        create_objects(f, empty=True)
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

<<<<<<< HEAD
# class IORead_Zarr_POSIX_local(target_zarr.ZarrStore):
#     def setup(self):
#         self.nz = getTestConfigValue("num_slices")
#         if not self.nz or self.nz <= 0: 
#             raise NotImplementedError("num_slices invalid")
#         self.create_objects(empty=False)

#     def time_readtest(self):
#         with h5py.File(self.path, 'r') as f:
#             dset = f[self.dset_name]
#             for i in range(self.nz):
#                 arr = dset[i,:,:]
#                 mean = arr.mean()
#                 if mean < 0.4 or mean > 0.6:
#                     msg = "mean of {} for slice: {} is unexpected".format(mean, i)
#                     raise ValueError(msg)

#     def time_fancycalculation(self):
#         return

#     def teardown(self):
#         self.rm_objects()


# class IOWrite_Zarr_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
#     def setup(self):
#         self.create_objects()
#         self.nz = getTestConfigValue("num_slices")
#         self.data = np.random.rand(*self.shape).astype(self.dtype)

#     def time_writetest(self):
#         with h5py.File(self.path, 'a') as f:
#             dset = f[self.dset_name]
#             for i in range(self.nz):
#                 dset[i,:,:] = self.data[i,:,:]

#     def teardown_files(self):
#         self.rm_objects()
=======
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


>>>>>>> a5e63b24390f34f6b5f84f084e93102143eb79be

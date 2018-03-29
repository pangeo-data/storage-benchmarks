"""
    Raw read/write performance of various backends and infrastructure
    TODO: should IO_raw just be combined with IO_numpy?


"""
from . import target_hdf5, target_hsds, getTestConfigValue
import random
import itertools
import numpy as np

_counter = itertools.count()

class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.nz = getTestConfigValue("num_slices")
        if not self.nz or self.nz <= 0: 
            raise NotImplementedError("num_slices invalid")
        self.create_objects(empty=False)

    def time_readtest(self):
        f = self.open(self.path, 'r')
        dset = f[self.dset_name]
        for i in range(self.nz):
            arr = dset[i,:,:]
            mean = arr.mean()
            if mean < 0.4 or mean > 0.6:
                msg = "mean of {} for slice: {} is unexpected".format(mean, i)
                raise ValueError(msg)
        f.close()
     
    def teardown(self):
        self.rm_objects()


class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        self.create_objects()
        self.nz = getTestConfigValue("num_slices")
        if self.nz <= 0:
            raise NotImplementedError("Skipping")
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        f = self.open(self.path, 'a')
        dset = f[self.dset_name]
        for i in range(self.nz):
            dset[i,:,:] = self.data[i,:,:]
        f.close()

    def teardown_files(self):
        self.rm_objects()

class IORead_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        if not self.username or not self.password or not self.endpoint:
            raise NotImplementedError("Missing config for HSDS tests")
        self.nz = getTestConfigValue("num_slices")
        if self.nz <= 0:
            raise NotImplementedError("Skipping")
        self.create_objects(empty=False)

    def time_readtest(self):
        f = self.open(self.path, 'r') 
        dset = f[self.dset_name]
        for i in range(self.nz):
            arr = dset[i,:,:]
            mean = arr.mean()
            if mean < 0.4 or mean > 0.6:
                msg = "mean of {} for slice: {} is unexpected".format(mean, i)
                raise ValueError(msg)
        f.close()

    def teardown(self):
        self.rm_objects()

class IOWrite_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        if not self.username or not self.password or not self.endpoint:
            raise NotImplementedError("Missing config for HSDS tests")
        self.nz = getTestConfigValue("num_slices")
        self.create_objects()
        self.data = np.random.rand(*self.shape).astype(self.dtype)

    def time_writetest(self):
        # Writing the entire 1GB in one h5pyd call is not yet supported, so write in slices
        f = self.open(self.path, 'a')  
        dset = f[self.dset_name]
        for i in range(self.nz):
            dset[i, :, :] = self.data[i, :, :]
         
    def teardown_files(self):
        self.rm_objects()

class IOSelect_LOCA_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        if not self.username or not self.password or not self.endpoint:
            raise NotImplementedError("Missing config for HSDS tests")
        self.year = getTestConfigValue("loca_year_start")

    def time_readslice(self):
        f = self.get_tasmax_file(self.year)
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

        f.close()

    def teardown(self):
        pass



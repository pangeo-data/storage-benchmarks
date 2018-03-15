'''
    IO Tests on Numpy performance

'''
from . import target_zarr, target_hdf5, target_hsds
import numpy as np

class IORead_zarr_POSIX_local(target_zarr.SingleZarrPOSIXFile):
    def setup(self):
        return

    def time_readtest(self):
        return

    def time_fancycalculation(self):
        return

class IOWrite_zarr_POSIX_local(target_zarr.SingleZarrPOSIXFile):
    def setup(self):
        return

    def time_writetest(self):
        return

    def time_fancywritecalculation(self):
        return


class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        return

    def time_readtest(self):
        return

    def time_fancycalculation(self):
        return


class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
    def setup(self):
        return

    def time_writetest(self):
        return

    def time_fancywritecalculation(self):
        return

class IORead_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        return

    def time_readtest(self):
        return

    def time_fancycalculation(self):
        return


class IOWrite_h5netcdf_HSDS(target_hsds.SingleHDF5HSDSFile):
    def setup(self):
        return

    def time_writetest(self):
        return

    def time_fancywritecalculation(self):
        return

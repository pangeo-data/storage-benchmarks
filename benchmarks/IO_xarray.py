'''
    Tests using Xarray datasets. 

'''

from . import target_zarr, target_hdf5
import os

import numpy as np
import xarray as xr


# class IORead_Random_Zarr_POSIXLocal(target_zarr.ZarrStore):
    
#     def setup(self):
#         self.create_objects(dset='xarray')
#         self.config_store(backend='POSIX')
#         self.ds.to_zarr(self.path)

#     def time_SyntheticRead(self):
#         xr.open_zarr(self.path).load()

#     def teardown(self):
#         self.rm_objects(backend='POSIX')

# class IOWrite_Random_Zarr_POSIXLocal(target_zarr.ZarrStore):
#     # Not specifying below will result in failure as ASV will repeatedly
#     # try to overwrite existing store causing Zarr to complain.
#     number = 1
#     warmup_time = 0.0

#     def setup(self):
#         self.create_objects(dset='xarray')
#         self.config_store(backend='POSIX')

#     def time_SyntheticWrite(self):
#         self.ds.to_zarr(self.path)

#     def teardown(self):
#         self.rm_objects(backend='POSIX')


# class Compute_Zarr_POSIXLocal(target_zarr.ZarrStore):

#     def setup(self):
#         self.create_objects(dset='xarray')
#         self.config_store(backend='POSIX')
#         self.ds.to_zarr(self.path)

#     def time_computemean(self):
#         xr.open_zarr(self.path).mean

#     def teardown(self):
#         self.rm_objects(backend='POSIX')


# class IORead_Random_ZarrGCS(target_zarr.ZarrStore):
#     number      = 1
#     warmup_time = 0.0
#     timeout = 300.0

#     def setup(self):
#         self.create_objects(dset='xarray')
#         self.config_store(backend='GCS')
#         self.ds.to_zarr(self.gcszarr_bucket)

#     def time_SyntheticRead(self):
#         xr.open_zarr(self.gcszarr_bucket).load()

#     def teardown(self):
#         self.rm_objects(backend='GCS')


# class IOWrite_Random_ZarrGCS(target_zarr.ZarrStore):
#     number      = 1
#     warmup_time = 0.0
#     timeout     = 300.0

#     def setup(self):
#         self.create_objects()
#         self.config_store(backend='GCS')

#     def time_SyntheticWrite(self):
#         self.ds.to_zarr(self.gcszarr_bucket)

#     def teardown(self):
#         self.rm_objects(backend='GCS')


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


# class IORead_Random_Zarr_GCSFUSE(target_zarr.ZarrStore):
#     number      = 1
#     warmup_time = 0.0
#     timeout = 300.0

#     def setup(self):
#         self.create_objects()
#         self.config_store(backend='GCS_FUSE')
#         self.ds.to_zarr(self.test_dir, 'w')

#     def time_SyntheticRead(self):
#         xr.open_zarr(self.test_dir).load()

#     def teardown(self):
#         self.rm_objects(backend='GCS_FUSE')
'''
    IO Tests on Xarray performance

'''

from . import target_zarr, target_hdf5
import os

import numpy as np
import xarray as xr

class IOWriteZarrPOSIXLocal(target_zarr.ZarrStore):
    def setup(self):
        self.make_ds()
        self.config_store(backend='POSIX')

    def time_SyntheticWrite(self):
        self.ds.to_zarr(self.path)

    def teardown_files(self):
        self.rm_store(backend='POSIX')


class IOReadZarrPOSIXLocal(target_zarr.ZarrStore):
    def setup(self):
        self.make_ds()
        self.config_store(backend='POSIX')
        self.ds.to_zarr(self.path)

    def time_SyntheticRead(self):
        xr.open_zarr(self.path).load()

    def teardown_files(self):
        self.rm_store(backend='POSIX')


class ComputeZarrPOSIXLocal(target_zarr.ZarrStore):
    def setup(self):
        self.make_ds()
        self.config_store(backend='POSIX')
        self.ds.to_zarr(self.path)

    def time_computemean(self):
        xr.open_zarr(self.path).mean

    def teardown_files(self):
        self.rm_store(backend='POSIX')


class IOWriteZarrGCS(target_zarr.ZarrStore):
    timeout = 300.0

    def setup(self):
        self.make_ds()
        self.config_store(backend='GCS')

    def time_SyntheticWrite(self):
        self.ds.to_zarr(self.gcs_store)

    def teardown_files(self):
        self.rm_store(backend='GCS')


class IOReadZarrGCS(target_zarr.ZarrStore):
    timeout = 300.0
    def setup(self):
        self.make_ds()
        self.config_store(backend='GCS')
        self.ds.to_zarr(self.gcs_store)

    def time_SyntheticRead(self):
        xr.open_zarr(self.gcs_store).load()

    def teardown_files(self):
        self.rm_store(backend='GCS')


class ComputeZarrGCS(target_zarr.ZarrStore):
    timeout = 300.0
    def setup(self):
        self.make_ds()
        self.config_store(backend='GCS')
        self.ds.to_zarr(self.gcs_store)

    def time_computemean(self):
        xr.open_zarr(self.gcs_store).mean

    def teardown_files(self):
        self.rm_store(backend='GCS')


class IOReadZarrGCS_FUSE(target_zarr.ZarrStore):
    timeout = 300.0
    def setup(self):
        self.make_ds()
        self.config_store(backend='GCS_FUSE')
        self.ds.to_zarr(self.test_dir, 'w')

    def time_SyntheticRead(self):
        # xr.open_zarr(self.test_dir).load()
        return

    def teardown(self):
        self.rm_store(backend='GCS_FUSE')
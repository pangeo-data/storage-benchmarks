'''
    IO Tests on Dask performance

'''
from subprocess import call
from . import target_zarr, target_hdf5
from . import benchmark_tools as bmt
from dask_kubernetes import KubeCluster
from dask.distributed import Client
from pathlib import Path

import dask_kubernetes
import os
import dask
import tempfile
import itertools
import shutil
import numpy as np
import dask.array as da
import h5py
import zarr
import tempfile


_counter = itertools.count()
_DATASET_NAME = "default"

def test_gcp():
   pod_conf = Path('/home/jovyan/worker-template.yaml')
   if not pod_conf.is_file():
   	raise NotImplementedError("Apparently not on GCP Pangeo environment... skipping") 

class IOWrite_Zarr():
    timeout = 60
    repeat = 1
    number = 1
    warmup_time = 0.0
    params = (['POSIX', 'GCS', 'FUSE'])
    param_names = ['backend']

    def setup(self, backend):
        test_gcp()

        cluster = KubeCluster(n_workers=10)
        cluster.adapt()    # or create and destroy workers dynamically based on workload
        client = Client(cluster)

        chunksize=(10, 1000, 1000)
        self.da = da.random.normal(10, 0.1, size=(100, 1000, 1000), 
                                   chunks=chunksize)

        self.da_size = np.round(self.da.nbytes / 1024**2, 2)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True, 
                                            chunksize=chunksize, shape=self.da.shape,
                                            dtype=self.da.dtype)
        self.target.get_temp_filepath()
        if backend == 'GCS':
            gsutil_arg = "gs://%s" % self.target.gcs_zarr
            call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])

    def time_synthetic_write(self, backend):
        with dask.set_options(get=dask.threaded.get):
            self.da.store(self.target.storage_obj, lock=False)

    def teardown(self, backend):
        self.target.rm_objects()
        return


# class IORead_zarr_POSIX_local(target_zarr.ZarrStore):
#     def setup(self):
#         return

#     def time_writetest(self):
#         return

#     def time_fancywritecalculation(self):
#         return


# class IOWrite_zarr_POSIX_local(target_zarr.ZarrStore):
#     def setup(self):
#         return

#     def time_writetest(self):
#         return

#     def time_fancywritecalculation(self):
#         return


# class ComputeSum_zarr_POSIX_local(target_zarr.ZarrStore):
#     # chunks
#     params = [256, 64]
#     param_names = ['chunksize']

#     def setup(self, chunksize):
#         self.make_ds()
#         self.config_store()
#         self.ds = zarr.open_array(self.path, mode='r')
#         self.da = da.from_array(self.ds, chunks=chunksize)

#     def time_sum(self, chunksize):
#         return self.da.sum().compute()

#     def teardown(self, chunksize):
#         self.rm_objects()


# class IORead_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
#     def setup(self):
#         return

#     def time_readtest(self):
#         return

#     def teardown(self):
#         return

# class IOWrite_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
#     def setup(self):
#         return

#     def time_writetest(self):
#         return

#     def teardown(self):
#         return

# class ComputeSum_h5netcdf_POSIX_local(target_hdf5.SingleHDF5POSIXFile):
#     # chunks
#     params = [256, 64]
#     param_names = ['chunksize']

#     def setup(self, chunksize):
#         self.create_objects(empty=False)
#         self.f = h5py.File(self.path)
#         self.ds = self.f[_DATASET_NAME]
#         self.da = da.from_array(self.ds, chunks=chunksize)

#     def time_sum(self, chunksize):
#         return self.da.sum().compute()

#     def teardown(self, chunksize):
#         self.f.close()
#         self.rm_objects()

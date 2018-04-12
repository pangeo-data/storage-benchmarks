"""Set of Dask based benchmarks

These ASV classes are meant to test the IO performance of various Dask/Xarray based 
calculations and operations against a variety of storage backends and architectures.


"""
from . import target_zarr, target_hdf5
from . import benchmark_tools as bmt

from dask.distributed import Client
from dask_kubernetes import KubeCluster
import dask
import dask.array as da
import dask.multiprocessing
import numpy as np

from subprocess import call
from pathlib import Path
import os
import tempfile
import itertools
import shutil
import zarr
import tempfile


_counter = itertools.count()
_DATASET_NAME = "default"

def test_gcp():
    """A very simple test to see if we're on Pangeo GCP environment
    Todo:
        Make this more robust

    Raises:
        NotImplementedError: Causes ASV to skip this test with assumption we're 
        not on Pangeo GCP environment

    """
   pod_conf = Path('/home/jovyan/worker-template.yaml')
   if not pod_conf.is_file():
   	raise NotImplementedError("Not on GCP Pangeo environment... skipping") 

class IOWrite_Zarr_GCP():
    """Synthetic random Dask data write test

    Generates a 10 GB dataset to benchmark write operations in a Dask/Kubernetes 
    Pangeo environment

    ASV Parameters:
        backend (str): Storage backend that will be used. e.g. POSIX fs, FUSE, etc.
        dask_get_opt (obj): Dask processing option. See Dask docs on set_options
        chunk_size (int): Dask chunk size across 'x' axis of dataset.
        n_workers (int): Number of Kubernetes Dask workers to spawn

    """
    timeout = 600
    repeat = 1
    number = 1
    warmup_time = 0.0
    params = (['GCS', 'FUSE'], [dask.get, dask.threaded.get, dask.multiprocessing.get],
              [1, 10, 100], [10, 50, 100])
    param_names = ['backend', 'dask_get_opt', 'chunk_size', 'n_workers']

    def setup(self, backend, dask_get_opt, chunk_size, n_workers):
        test_gcp()

        cluster = KubeCluster(n_workers=n_workers)
        cluster.adapt()    # or create and destroy workers dynamically based on workload
        client = Client(cluster)

        chunksize=(chunk_size, 1000, 1000)
        self.da = da.random.normal(10, 0.1, size=(1100, 1100, 1100), 
                                   chunks=chunksize)

        self.da_size = np.round(self.da.nbytes / 1024**2, 2)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True, 
                                            chunksize=chunksize, shape=self.da.shape,
                                            dtype=self.da.dtype)
        self.target.get_temp_filepath()
        if backend == 'GCS':
            gsutil_arg = "gs://%s" % self.target.gcs_zarr
            call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])

    def time_synthetic_write(self, backend, dask_get_opt, chunk_size, n_workers):
        benchmark_name = "Random Dask write of %s" % self.da_size
        with dask.set_options(get=dask_get_opt):
            self.da.store(self.target.storage_obj, lock=False)

    def teardown(self, backend, dask_get_opt, chunk_size, n_workers):
        self.target.rm_objects()



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

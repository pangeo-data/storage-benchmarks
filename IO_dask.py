"""Dask IO performance.

These ASV classes are meant to test the IO performance of various Dask/Xarray
based calculations and operations against a variety of storage backends and
architectures.

ASV Parameters:
    backend (str): Storage backend that will be used. e.g. POSIX fs, FUSE,
        etc.

    dask_get_opt (obj): Dask processing option. See Dask docs on
        set_options.

    chunk_size (int): Dask chunk size across 'x' axis of
        dataset.

    n_workers (int): Number of Kubernetes Dask workers to spawn

"""
from . import target_zarr, target_hdf5, getTestConfigValue
from . import benchmark_tools as bmt

from dask.distributed import Client
from dask_kubernetes import KubeCluster
from time import sleep
import dask
import dask.array as da
import dask.multiprocessing
import numpy as np
import timeit


from subprocess import call
from time import sleep
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
        return False
    else:
        return True


def cluster_wait(client, n_workers):
    """Delay process until Kubernetes cluster has provisioned worker pods"""
    while len(client.cluster.scheduler.workers) < n_workers:
        print("Provisioning worker pods. %s/%s " %
              (len(client.cluster.scheduler.workers), n_workers))
        sleep(2)


class Zarr_GCP_write_10GB():
    """Synthetic random Dask data write test

    """
    timer = timeit.default_timer
    timeout = 600
    repeat = 5
    number = 5
    warmup_time = 0.0
    params = (['GCS'], [1, 5, 10], [5, 10, 20, 40, 80])
    #params = (['GCS'], [5], [5])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.is_gcp = test_gcp()
        # Otherwise, ASV will still try to configure variables
        if self.is_gcp:
            self.cluster = KubeCluster(n_workers=n_workers)
            self.client = Client(self.cluster)
            cluster_wait(self.client, n_workers)

            self.chunks=(n_chunks, 1000, 1000)
            self.da = da.random.normal(10, 0.1, size=(1350, 1000, 1000), 
                                       chunks=self.chunks)
            self.da_size = np.round(self.da.nbytes / 1024**3, 2) # in gigabytes
            self.target = target_zarr.ZarrStore(backend=backend, dask=True,
                                            chunksize=self.chunks,
                                            shape=self.da.shape,
                                            dtype=self.da.dtype)
            # Maybe combine above and below methods
            # so init does all this?
            # seems a little superfluous?
            self.target.get_temp_filepath()
        else:
            raise NotImplementedError("Not on GCP Pangeo environment... skipping")

    def time_synthetic_write(self, backend, n_chunks, n_workers):
        if self.is_gcp:
            self.da.store(self.target.storage_obj, lock=False)
        
    def teardown(self, backend, n_chunks, n_workers):
        if self.is_gcp:
            self.cluster.close()
            self.target.rm_objects()


class Zarr_GCP_Dask_compute_10GB():
    """Synthetic random Dask data read and computation test

    """
    timer = timeit.default_timer
    timeout = 600
    repeat = 5
    number = 5
    warmup_time = 0.0
    params = (['GCS'], [1, 5, 10], [5, 10, 20, 40, 80])
    #params = (['GCS'], [5], [5])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def setup(self, backend, n_chunks, n_workers):
        self.is_gcp = test_gcp()
        # Otherwise, ASV will still try to configure variables
        if self.is_gcp:
            self.cluster = KubeCluster(n_workers=n_workers)
            self.client = Client(self.cluster)
            cluster_wait(self.client, n_workers)

            self.chunks=(n_chunks, 1000, 1000)
            self.da = da.random.normal(10, 0.1, size=(1350, 1000, 1000),
                                       chunks=self.chunks)
            self.da_size = np.round(self.da.nbytes / 1024**3, 2) # in gigabytes
            self.target = target_zarr.ZarrStore(backend=backend, dask=True,
                                                chunksize=self.chunks,
                                                shape=self.da.shape,
                                                dtype=self.da.dtype)
            # Maybe combine above and below methods so init does all this?
            # seems a little superfluous?
            self.target.get_temp_filepath()
            self.da.store(self.target.storage_obj, lock=False)
        else:
            raise NotImplementedError("Not on GCP Pangeo environment...\
                                      skipping")

    def time_mean(self, backend, n_chunks, n_workers):
        if self.is_gcp:
            pretty_name = "10 GB - compute mean"
            test_da = da.from_array(self.target.storage_obj, chunks=self.chunks)
            test_da.mean().compute()

    def teardown(self, backend, n_chunks, n_workers):
        if self.is_gcp:
            self.cluster.close()
            self.target.rm_objects()

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

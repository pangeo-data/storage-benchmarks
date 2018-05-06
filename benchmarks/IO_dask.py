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
import dask
import dask.array as da
import dask.multiprocessing
import numpy as np
import xarray as xr

from subprocess import call
from time import sleep
from pathlib import Path
import os
import tempfile
import itertools
import shutil
import timeit
import zarr
import tempfile

_counter = itertools.count()
_DATASET_NAME = "default"


def test_gcp(func):
    """A very simple test to see if we're on Pangeo GCP environment
    Todo:
        Make this more robust

    Raises:
        NotImplementedError: Causes ASV to skip this test with assumption we're
        not on Pangeo GCP environment

    """
    pod_conf = Path('/home/jovyan/worker-template.yaml')

    def func_wrapper(*args, **kwargs):
        if not pod_conf.is_file():
            if func.__name__ == 'setup':
                raise NotImplementedError("Not on GCP Pangeo environment... skipping")
            else:
                return
        else:
            func(*args, **kwargs)
    return func_wrapper

def cluster_wait(client, n_workers):
    """Delay process until Kubernetes cluster has provisioned worker pods"""
    while len(client.cluster.scheduler.workers) < n_workers:
        sleep(2)


class Zarr_GCP_write_10GB():
    """Synthetic random Dask data write test

    """
    timer = timeit.default_timer
    timeout = 1200
    repeat = 1
    number = 5
    warmup_time = 0.0
    params = (['GCS'], [5, 10], [20, 40, 60, 80, 100])
    #params = (['GCS'], [1], [40])
    param_names = ['backend', 'n_chunks', 'n_workers']

    @test_gcp
    def setup(self, backend, n_chunks, n_workers):
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
        self.target.get_temp_filepath()

    @test_gcp
    def time_synthetic_write(self, backend, n_chunks, n_workers):
        self.da.store(self.target.storage_obj, lock=False)
    
    @test_gcp
    def teardown(self, backend, n_chunks, n_workers):
        if self.is_gcp:
            self.cluster.close()
            self.target.rm_objects()

class Zarr_GCP_LLC4320():
    """Zarr GCP tests on LLC4320 Datasets

    """
    timer = timeit.default_timer
    timeout = 1200
    repeat = 1
    number = 5
    warmup_time = 0.0
    params = (['GCS', 'FUSE'], [20, 40, 60, 80, 100])
    param_names = ['backend', 'n_workers']

    @test_gcp
    def setup(self, backend, n_workers):

        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        cluster_wait(self.client, n_workers)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True)

    @test_gcp
    def time_read(self, backend, n_workers):
        """Use potential temp as a proxy to load entire data
           set and get throughput
        """
        if backend == 'GCS':
            self.llc_ds = self.target.open_store('llc4320_zarr')
        elif backend == 'FUSE':
            self.llc_ds = self.target.open_store('llc4320_zarr_fuse')
        ds = self.llc_ds.persist()
        ds.Theta.max().compute()

    @test_gcp
    def time_load_array_compute_SST_time_mean(self, backend, n_workers):
        """Time to persist an array in dataset and compute the mean

        """
        if backend == 'GCS':
            self.llc_ds = self.target.open_store('llc4320_zarr')
        elif backend == 'FUSE':
            self.llc_ds = self.target.open_store('llc4320_zarr_fuse')
        ds_theta = self.llc_ds.Theta.persist()
        # Need to redo number of chunks so we can saturate workersk
        ds_theta.mean().compute() # SST mean across time

    @test_gcp
    def teardown(self, backend, n_workers):
        self.cluster.close()


class NetCDF_GCP_LLC4320():
    """LLC4320 NetCDF files from GCS FUSE mount
    """
    timer = timeit.default_timer
    timeout = 1200
    repeat = 1
    number = 5
    warmup_time = 0.0
    params = ([1], [20, 40, 60, 80, 100])
    param_names = ['n_chunks', 'n_workers']

    @test_gcp
    def setup(self, n_chunks, n_workers):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        cluster_wait(self.client, n_workers)
        self.llc_ds = xr.open_mfdataset('/gcs/storage-benchmarks/llc4320_netcdf/*.nc',
                                        decode_cf=False, autoclose=True,
                                        chunks={'k': 1, 'k_l': n_chunks})

    @test_gcp
    def time_read(self, backend, n_workers):
        """Use potential temp as a proxy to load entire data
           set and get throughput
        """
        ds = self.llc_ds.persist()
        ds.Theta.max().compute()

    @test_gcp
    def time_load_array_compute_SST_time_mean(self, backend, n_workers):
        """Time to persist an array in dataset and compute the mean

        """
        ds_theta = self.llc_ds.Theta.persist()
        ds_theta.mean().compute() # SST mean across time

    @test_gcp
    def teardown(self, backend, n_workers):
        self.cluster.close()

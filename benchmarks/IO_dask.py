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

from os.path import abspath, dirname, join
from subprocess import call, Popen
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
_retries = 3

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
    """Delay process until Kubernetes cluster has provisioned worker pods
       and clean up completed pods in the process.
    
    """
    start = timeit.default_timer()
    wait_threshold = 300
    worker_threshold = n_workers * .95

    while len(client.cluster.scheduler.workers) < n_workers:
        kill_daskpods()
        sleep(2)
        elapsed = timeit.default_timer() - start
        # If we're getting close to timeout but cluster is mostly provisioned,
        # just break out or test will fail
        if elapsed > wait_threshold and len(client.cluster.scheduler.workers) >= worker_threshold:
            break

def kill_daskpods():
    """Invoke kill script to clean up completed Dask pods provisioned for 
       user running this test

       HACK: this needs improvement

    """

    path = abspath(join(dirname(__file__), '../bin/kill_daskpods.sh'))
    call([ path ])

class Zarr_GCP_synthetic_write():
    """Synthetic random Dask data write test

    """
    timer = timeit.default_timer
    timeout = 600
    repeat = 1
    number = 8
    warmup_time = 0.0
    params = (['GCS'], [1, 5, 10], [20, 40, 80, 120])
    #params = (['GCS'], [1, 5], [50, 100])
    param_names = ['backend', 'n_chunks', 'n_workers']

    @test_gcp
    def setup(self, backend, n_chunks, n_workers):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        cluster_wait(self.client, n_workers)
        self.chunks = (n_chunks, 3000, 3000)
        self.da = da.random.normal(10, 0.1, size=(1000, 3000, 3000), 
                                       chunks=self.chunks)
        _rand_da_nbytes = self.da.nbytes
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
        self.cluster.close()
        del self.da
        self.target.rm_objects()


class Zarr_GCP_LLC4320():
    """Zarr GCP tests on LLC4320 Datasets

    """
    timer = timeit.default_timer
    timeout = 2400
    #repeat = 3
    number = 3
    warmup_time = 0.0
    params = (['GCS'], [1], [20, 40, 80, 120])
    #params = (['GCS'], [1], [120])
    param_names = ['backend', 'n_chunks', 'n_workers']

    @test_gcp
    def setup(self, backend, n_chunks, n_workers):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        cluster_wait(self.client, n_workers)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True)

    @test_gcp
    def time_load_array_compute_theta_mean(self, backend, n_chunks, n_workers):
        if backend == 'GCS':
            self.llc_ds = self.target.open_store('llc4320_zarr')
        elif backend == 'FUSE':
            self.llc_ds = self.target.open_store('llc4320_zarr_fuse')
        ds_theta = self.llc_ds.Theta
        ds_theta.mean().load(retries=_retries) 
        del ds_theta

    @test_gcp
    def teardown(self, backend, n_chunks, n_workers):
        self.cluster.close()


class NetCDF_GCP_LLC4320():
    """LLC4320 NetCDF files from GCS FUSE mount
    """
    timer = timeit.default_timer
    timeout = 3600
    repeat = 1
    number = 8
    warmup_time = 0.0
    params = (['FUSE'], [10, 90], [40, 80, 120])
    #params = (['FUSE'], [1], [50])
    param_names = ['backend', 'n_chunks', 'n_workers']

    @test_gcp
    def setup(self, backend, n_chunks, n_workers):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        cluster_wait(self.client, n_workers)
        self.llc_ds = xr.open_mfdataset('/gcs/storage-benchmarks/llc4320_netcdf/*.nc',
                                        decode_cf=False, autoclose=True,
                                        chunks={'k': n_chunks, 'k_l': n_chunks})

    @test_gcp
    def time_load_array_compute_theta_mean(self, backend, n_chunks, n_workers):
        ds_theta = self.llc_ds.Theta
        ds_theta.mean().load(retries=_retries) # SST mean across time
        del ds_theta

    @test_gcp
    def teardown(self, backend, n_chunks, n_workers):
        self.cluster.close()


class Report_dataset_sizes():
    number = 1
    repeat = 1
    warmup_time = 0.0
    params = (['ALL'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    @test_gcp
    def track_Zarr_GCP_synthetic_write_dataset_size(self, backend, n_chunks, n_workers):
        # HACK make it cleaner later
        chunks = (1, 3000, 3000)
        size = (1000, 3000, 3000)
        dask_arr = da.random.normal(10, 0.1, size=size, chunks=chunks)
        return dask_arr.nbytes / 1024**3

    @test_gcp
    def track_Zarr_GCP_LLC4320_dataset_size(self, backend, n_chunks, n_workers):
        target = target_zarr.ZarrStore(backend='GCS', dask=True)
        llc_ds = target.open_store('llc4320_zarr')
        return llc_ds.nbytes / 1024**3

    @test_gcp
    def track_NetCDF_GCP_LLC4320_dataset_size(self, backend, n_chunks, n_workers):
        llc_ds = xr.open_mfdataset('/gcs/storage-benchmarks/llc4320_netcdf/*.nc',
                                   decode_cf=False, autoclose=True)
        return llc_ds.nbytes / 1024**3

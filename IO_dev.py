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

class NetCDF_GCP_LLC4320():
    """LLC4320 NetCDF files from GCS FUSE mount
    """
    timer = timeit.default_timer
    timeout = 1200
    repeat = 1
    number = 5
    warmup_time = 0.0 
    params = ([1, 10], [20, 40])
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
        ds_theta[:, 0].mean().compute() # SST mean across time

    @test_gcp
    def teardown(self, backend, n_workers):
        self.cluster.close()

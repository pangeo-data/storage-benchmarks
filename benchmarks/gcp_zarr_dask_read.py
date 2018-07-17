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
        # kill_daskpods()
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

class Zarr_GCP_LLC4320():
    """Zarr GCP tests on LLC4320 Datasets

    """
    timer = timeit.default_timer
    timeout = 3600
    repeat = 1
    number = 1
    warmup_time = 0.0
    params = (['GCS'], [1, 5, 10], [60, 80, 100, 120, 140, 160])
    #params = (['GCS'], [5], [120])
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
            self.llc_ds = self.target.open_store('llc4320_zarr_100')
        elif backend == 'FUSE':
            self.llc_ds = self.target.open_store('llc4320_zarr_fuse')
        ds_theta = self.llc_ds.Theta
        ds_theta.max().load(retries=_retries) 
        del ds_theta

    @test_gcp
    def teardown(self, backend, n_chunks, n_workers):
        self.cluster.close()

class Report_dataset_sizes():
    number = 1
    timeout = 1200
    repeat = 1
    warmup_time = 0.0
    params = (['ALL'], [1], [1])
    param_names = ['backend', 'n_chunks', 'n_workers']

    def track_Zarr_GCP_LLC4320_dataset_size(self, backend, n_chunks, n_workers):
        target = target_zarr.ZarrStore(backend='GCS', dask=True)
        llc_ds = target.open_store('llc4320_zarr_100')
        return llc_ds.nbytes / 1024**3

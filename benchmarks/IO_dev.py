from . import target_zarr, target_hdf5, getTestConfigValue
from . import benchmark_tools as bmt

from dask.distributed import Client
from dask_kubernetes import KubeCluster
import dask
import dask.array as da
import dask.multiprocessing
import numpy as np

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

class Zarr_GCP_LLC4320():
    """Zarr GCP tests on LLC4320 Datasets

    """
    timer = timeit.default_timer
    timeout = 600
    repeat = 5
    number = 5
    warmup_time = 0.0
    params = (['FUSE'], [40])
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
           set and get throughput time
        """
        if backend == 'GCS':
            self.llc_ds = self.target.open_store('llc4320_zarr')
        elif backend == 'FUSE':
            self.llc_ds = self.target.open_store('llc4320_zarr_fuse')
        ds = self.llc_ds.persist()
        ds = self.llc_ds.W.max().compute()


#    @test_gcp
#    def time_load_array_compute_mean(self, backend, n_workers):
#        """Time to persist an array in dataset and compute the mean
#
#        """
#        self.llc_ds = self.target.open_gcsfs('storage-benchmarks/llc4320_zarr')
#        ds_W = self.llc_ds.W.persist()
#        ds_W.mean(dim='time').compute()

    @test_gcp
    def teardown(self, backend, n_workers):
        self.cluster.close()

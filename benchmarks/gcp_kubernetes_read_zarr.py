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

from . import target_zarr
from . import benchmark_tools as bmt
from . import getTestConfigValue
from dask.distributed import Client
from dask_kubernetes import KubeCluster
import itertools
import numpy as np
import timeit
import xarray as xr
import zarr

RETRIES = 5
DS_STORE = 'llc4320_zarr_1000'
RUNS = getTestConfigValue('n_runs')

class llc4320_benchmarks():
    """Zarr GCP tests on LLC4320 Datasets

    """
    timer = timeit.default_timer
    timeout = 3600
    repeat = 1
    number = 1
    warmup_time = 0.0
    run_nums = np.arange(1, RUNS + 1)
    params = (['GCS'], [1], [60, 80, 100, 120, 140, 160], run_nums)
    #params = (['GCS'], [1], [50, 60], run_nums)
    #params = getTestConfigValue("gcp_kubernetes_read_zarr.llc4320_benchmarks")
    param_names = ['backend', 'z_chunksize', 'n_workers', 'run_num']

    @bmt.test_gcp
    def setup(self, backend, z_chunksize, n_workers, run_num):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        bmt.cluster_wait(self.client, n_workers)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True)
        # Open Zarr DS
        self.ds_zarr = self.target.open_store(DS_STORE)
        self.ds_zarr_theta = self.ds_zarr.Theta

    @bmt.test_gcp
    def time_read(self, backend, z_chunksize, n_workers, run_num):
        self.ds_zarr_theta.max().load(retries=RETRIES) 

    @bmt.test_gcp
    def teardown(self, backend, z_chunksize, n_workers, run_num):
        del self.ds_zarr_theta
        self.cluster.close()

class llc4320_ds_size():
    number = 1
    timeout = 300
    repeat = 1
    warmup_time = 0.0

    def track_megabytes(self):
        target = target_zarr.ZarrStore(backend='GCS', dask=True)
        llc_ds = target.open_store(DS_STORE)
        return llc_ds.nbytes / 2**20 

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
from . import benchmark_tools as bmt
from . import getTestConfigValue
from . import target_zarr

from dask.distributed import Client
from dask_kubernetes import KubeCluster
import numpy as np
import timeit
import xarray as xr

RETRIES = 5
DS_FILES = '/gcs/storage-benchmarks/llc4320_netcdf_10/*.nc'
RUNS = getTestConfigValue('n_runs')

class llc4320_benchmarks():
    """netCDF GCP tests on LLC4320 Datasets

    """
    timer = timeit.default_timer
    timeout = 3600
    repeat = 1
    number = 1
    warmup_time = 0.0
    run_nums = np.arange(1, RUNS + 1)
    #params = (['FUSE'], [1, 5, 10], [60, 80, 100, 120, 140, 160], run_nums)
    params = (['FUSE'], [10], [60], run_nums)
    param_names = ['backend', 'z_chunksize', 'n_workers', 'run_num']

    @bmt.test_gcp
    def setup(self, backend, z_chunksize, n_workers, run_num):
        self.cluster = KubeCluster(n_workers=n_workers)
        self.client = Client(self.cluster)
        bmt.cluster_wait(self.client, n_workers)
        self.target = target_zarr.ZarrStore(backend=backend, dask=True)
        # Open netCDF DS
        self.ds_netcdf = xr.open_mfdataset(DS_FILES, decode_cf=False, 
                                           autoclose=True, 
                                           chunks={'Z': z_chunksize})
        self.ds_netcdf_theta = self.ds_netcdf.Theta

    @bmt.test_gcp
    def time_read(self, backend, z_chunksize, n_workers, run_num):
        self.ds_netcdf_theta.max().load(retries=RETRIES)

    @bmt.test_gcp
    def teardown(self, backend, z_chunksize, n_workers, run_num):
        del self.ds_netcdf_theta
        self.cluster.close()

class llc4320_ds_size():
    number = 1
    timeout = 1200
    repeat = 1
    warmup_time = 0.0

    def track_megabytes(self):
        ds = xr.open_mfdataset(DS_FILES, decode_cf=False, autoclose=True)
        return ds.nbytes / 2**20

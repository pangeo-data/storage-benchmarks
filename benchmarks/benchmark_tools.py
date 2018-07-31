"""Tools module for storage-benchmark tests
   
   This module provides tools to generate datasets or random data for use
   in benchmark calculations. Tools used to manipulate this data or otherwise
   run helper calculations are present here as well.


"""
from . import getTestConfigValue
from time import sleep
import timeit
import numpy as np
import pandas as pd
import xarray as xr


from pathlib import Path

_DATASET_NAME = "default"

def test_gcp(func):
    """A very simple test to see if we're on Pangeo GCP environment

    Check existence of jovyan homedir and worker-template to determine if on
    Kubernetes based cluster. Not very robust, but ok for now.

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

def randn(shape, frac_nan=None, chunks=None, seed=0):
    rng = np.random.RandomState(seed)
    if chunks is None:
        x = rng.standard_normal(shape)
    else:
        import dask.array as da
        rng = da.random.RandomState(seed)
        x = rng.standard_normal(shape, chunks=chunks)

    if frac_nan is not None:
        inds = rng.choice(range(x.size), int(x.size * frac_nan))
        x.flat[inds] = np.nan

    return x

def randint(low, high=None, size=None, frac_minus=None, seed=0):
    rng = np.random.RandomState(seed)
    x = rng.randint(low, high, size)
    if frac_minus is not None:
        inds = rng.choice(range(x.size), int(x.size * frac_minus))
        x.flat[inds] = -1

    return x

def rand_numpy(f, nz=None, empty=True):
    """
    Generate random 3D Numpy dataset.

    :params;

    """
    if nz == None:
        nz = getTestConfigValue("np_nz")
    if not nz or nz <= 0: 
        raise NotImplementedError("num_slices invalid")
    nx = getTestConfigValue("np_nx")
    ny = getTestConfigValue("np_ny")
    dtype = 'f8'
    # Create a dataset
    dset = f.create_dataset(_DATASET_NAME, shape=(nz,ny,nx), dtype=dtype)

    if not empty:
        # fill in some random data
        data = np.random.rand(*dset.shape).astype(dset.dtype)
        for i in range(nz):
            dset[i, :, :] = data[i, :, :]

def rand_xarray(nt=None):
    """
    Generate synthetic geoscience-like Xarray dataset filled with random 
    data.

    :param ds: dataset that gets generated.
    :param nt: number of timesteps for data. Primary control over how large
               the dataset is.
    :returns: A synthetic xarray dataset that mimics geodata.

    """

    ds = xr.Dataset()
    if nt == None:
        nt = getTestConfigValue("xr_nt")
    ny = getTestConfigValue("xr_ny")
    nx = getTestConfigValue("xr_nx")
    block_chunks = {'time': nt / 4,
                             'lon': nx / 3,
                             'lat': ny / 3}

    time_chunks = {'time': int(nt / 36)}

    times = pd.date_range('1970-01-01', periods=nt, freq='D')
    lons = xr.DataArray(np.linspace(0, 360, nx), dims=('lon', ),
                        attrs={'units': 'degrees east',
                               'long_name': 'longitude'})
    lats = xr.DataArray(np.linspace(-90, 90, ny), dims=('lat', ),
                        attrs={'units': 'degrees north',
                               'long_name': 'latitude'})
    ds['foo'] = xr.DataArray(randn((nt, nx, ny), frac_nan=0.2),
                             coords={'lon': lons, 'lat': lats,'time': times},
                             dims=('time', 'lon', 'lat'),
                             name='foo', encoding=None,
                             attrs={'units': 'foo units',
                                    'description': 'a description'})
    ds['bar'] = xr.DataArray(randn((nt, nx, ny), frac_nan=0.2),
                             coords={'lon': lons, 'lat': lats, 'time': times},
                             dims=('time', 'lon', 'lat'),
                             name='bar', encoding=None,
                             attrs={'units': 'bar units',
                                    'description': 'a description'})
    ds['baz'] = xr.DataArray(randn((nx, ny), frac_nan=0.2).astype(np.float32),
                             coords={'lon': lons, 'lat': lats},
                             dims=('lon', 'lat'),
                             name='baz', encoding=None,
                             attrs={'units': 'baz units',
                                    'description': 'a description'})

    ds.attrs = {'history': 'created for xarray benchmarking'}

    oinds = {'time': randint(0, nt, 120),
             'lon': randint(0, nx, 20),
             'lat': randint(0, ny, 10)}
    vinds = {'time': xr.DataArray(randint(0, nt, 120), dims='x'),
             'lon': xr.DataArray(randint(0, nx, 120), dims='x'),
             'lat': slice(3, 20)}

    return ds

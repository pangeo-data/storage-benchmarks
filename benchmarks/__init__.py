from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import itertools
import yaml
import os
import numpy as np
import pandas as pd
import xarray as xr

_counter = itertools.count()


def parameterized(names, params):
    def decorator(func):
        func.param_names = names
        func.params = params
        return func
    return decorator


def requires_dask():
    try:
        import dask  # noqa
    except ImportError:
        raise NotImplementedError


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

def getTestConfigValue(k):
    val = None
    # check to see if we have an environment override
    if k.upper() in os.environ:
        val = os.environ[k.upper()]
    else:
        cwd = os.getenv("PWD")  # not the same as os.getcwd()
        config_file_path = os.path.join(cwd, "test.conf.yaml")
        with open(config_file_path, "r") as f:
            cfg = yaml.load(f)
        if k in cfg:
            val = cfg[k]
    return val

def create_numpy_random(num_slices=None):
    """
    Generate random Numpy array

    """

    if num_slices == None:
        num_slices = getTestConfigValue("num_slices")
    nz = getTestConfigValue("num_slices")
    ny = 256
    nx = 512
    shape = (nz, ny, nx)
    dtype = 'f8'
    data = np.random.rand(*shape).astype(dtype)

    return data

def create_xarray_random(num_time_slices=None):
    """
    Generate synthetic geoscience-like Xarray dataset filled with random 
    data.

    """

    if num_time_slices == None:
        num_time_slices = getTestConfigValue("num_time_slices")
    ds = xr.Dataset()
    nt = int(num_time_slices)
    nx = 90
    ny = 45
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


     

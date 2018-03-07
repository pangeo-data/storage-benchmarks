import os
import tempfile
import itertools
import shutil

import numpy as np
import dask.array as da
import zarr

_counter = itertools.count()
_DATASET_NAME = "default"

class SingleZarrFile(object):
    """Base class for single Zarr File Reading."""
    # modeled after xarray asv benchmarks
    # https://github.com/pydata/xarray/blob/master/asv_bench/benchmarks/dataset_io.py

    def setup_files(self):
        self.nz = 1024
        self.ny = 256
        self.nx = 512
        self.shape = (self.nz, self.ny, self.nx)
        self.dtype = 'f8'
        data = np.random.rand(*self.shape).astype(self.dtype)

        # https://github.com/pydata/xarray/blob/master/xarray/tests/test_backends.py#L773
        suffix = '.zarr'
        self.temp_dir = tempfile.mkdtemp()
        self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))
        zarrfile = zarr.open_array(self.path, mode='w', shape=(self.shape))
        zarrfile[...] = data[...]

    def teardown_files(self):
        shutil.rmtree(self.temp_dir)

class DaskChunkedReduce(SingleZarrFile):

    # chunks
    params = [256, 64]
    param_names = ['chunksize']

    def setup(self, chunksize):
        self.setup_files()
        self.ds = zarr.open_array(self.path, mode='r')
        self.da = da.from_array(self.ds, chunks=chunksize)

    def time_sum(self, chunksize):
        return self.da.sum().compute()

    def teardown(self, chunksize):
        return
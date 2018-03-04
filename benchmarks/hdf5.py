import os
import tempfile
import itertools
import shutil

import numpy as np
import dask.array as da
import h5py

_counter = itertools.count()
_DATASET_NAME = "default"

class SingleHDF5File(object):
    """Base class for single HDF5 File Reading."""
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
        suffix = '.h5'
        self.temp_dir = tempfile.mkdtemp()
        self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))
        h5file = h5py.File(self.path, 'w')

        h5file.create_dataset(_DATASET_NAME, data=data)
        # are these both necessary
        h5file.flush()
        h5file.close()

    def teardown_files(self):
        shutil.rmtree(self.temp_dir)


class DaskChunkedReduce(SingleHDF5File):

    # chunks
    params = [256, 64]
    param_names = ['chunksize']

    def setup(self, chunksize):
        self.setup_files()
        self.f = h5py.File(self.path)
        self.ds = self.f[_DATASET_NAME]
        self.da = da.from_array(self.ds, chunks=chunksize)

    def time_sum(self, chunksize):
        return self.da.sum().compute()

    def teardown(self, chunksize):
        self.f.close()
        self.teardown_files()



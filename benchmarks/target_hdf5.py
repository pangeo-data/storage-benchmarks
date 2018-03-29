""""
   Set up HDF5 Datasets on various backends

"""

import os
import tempfile
import itertools
import shutil
import numpy as np
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py
import h5py
from . import getTestConfigValue

_counter = itertools.count()
_DATASET_NAME = "default"


class SingleHDF5POSIXFile(object):
    """
    Single HDF5 POSIX File     

    """
    def __init__(self):
        self.temp_dir = None
        self.path = None
    
    def open(self, path, mode):
        return h5py.File(path, mode)

    def create_objects(self, empty=True):
        suffix = '.h5'
        self.temp_dir = tempfile.mkdtemp()
        self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))
        h5file = self.open(self.path, 'w')
             
        self.nz = getTestConfigValue("num_slices")
        self.ny = 256
        self.nx = 512
        self.shape = (self.nz, self.ny, self.nx)
        self.dtype = 'f8'
        # Create a 1GB dataset
        data = np.random.rand(*self.shape).astype(self.dtype)
        dset = h5file.create_dataset(_DATASET_NAME, self.shape, dtype = self.dtype)
        self.dset_name = _DATASET_NAME
        # Writing the entire dataset in one h5pyd call is not yet supported for large datasets, so write in slices
        if not empty:
            for i in range(self.nz):
                dset[i, :, :] = data[i, :, :]

        h5file.close()

    def rm_objects(self):
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)
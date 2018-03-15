""""
   Set up HDF5 Datasets on various backends



"""

import os
import tempfile
import itertools
import shutil
import numpy as np
import h5py

_counter = itertools.count()
_DATASET_NAME = "default"


class SingleHDF5POSIXFile(object):
    """
    Single HDF5 POSIX File     

    """       
    def create_objects(self, empty=True):
       
        suffix = '.h5'
        self.temp_dir = tempfile.mkdtemp()
        self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))
        self.h5file = h5py.File(self.path, 'w')

        if not empty:
            self.nz = 1024
            self.ny = 256
            self.nx = 512
            self.shape = (self.nz, self.ny, self.nx)
            self.dtype = 'f8'
            data = np.random.rand(*self.shape).astype(self.dtype)
            self.h5file.create_dataset(_DATASET_NAME, data=data)
            # are these both necessary
            self.h5file.flush()
            self.h5file.close()

    def rm_objects(self):
        shutil.rmtree(self.temp_dir)
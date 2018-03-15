""""
   Set up Zarr Datasets on various backends



"""

import os
import tempfile
import itertools
import shutil
import numpy as np
import zarr

_counter = itertools.count()
_DATASET_NAME = "default"


class SingleZarrPOSIXFile(object):
    """
    Single Zarr POSIX File.
    
    """
    def create_objects(self, empty=True):
        suffix = '.zarr'
        self.temp_dir = tempfile.mkdtemp()
        self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))
        if not empty:
            self.nz = 1024
            self.ny = 256
            self.nx = 512
            self.shape = (self.nz, self.ny, self.nx)
            self.dtype = 'f8'
            data = np.random.rand(*self.shape).astype(self.dtype)
            zarrfile = zarr.open_array(self.path, mode='w', shape=(self.shape))
            zarrfile[...] = data[...]

    def rm_objects(self):
        shutil.rmtree(self.temp_dir)


class ZarrGoogleCloud(object):
    """
        Prototype class for single Zarr file on Google Cloud
    """

    def setup_creds():
        return

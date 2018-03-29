""""
   Set up HDF5 Datasets on various backends

"""

import os
import tempfile
import itertools
import shutil
import warnings
with warnings.catch_warnings():
    warnings.filterwarnings("ignore",category=FutureWarning)
    import h5py
 

_counter = itertools.count()


class SingleHDF5POSIXFile(object):
    """
    Single HDF5 POSIX File     

    """
    def __init__(self):
        self.temp_dir = None
        self.suffix = ".h5"

    def get_temp_filepath(self):
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()
        filename = 'temp-{}{}'.format(next(_counter), self.suffix)
        filepath = os.path.join(self.temp_dir, filename)
        return filepath
    
    def open(self, path, mode):
        return h5py.File(path, mode)

    def rm_objects(self):
        return
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None
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
from . import getTestConfigValue
 

_counter = itertools.count()

_LOCA_NAME = "tasmax_day_ACCESS1-0_historical_r1i1p1_19500101-19501231.LOCA_2016-04-02.16th.nc"



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

    def get_tasmax_filepath(self, year=1950):
        loca_file_directory = getTestConfigValue("loca_posix_dir")
        if not loca_file_directory:
            raise NotImplementedError("No directory for LOCA files")
        
        # TBD: Get different files for different years
        filepath = os.path.join(loca_file_directory, _LOCA_NAME)
        try:
            self.open(filepath, 'r')
        except IOError:
            raise NotImplementedError("File: {} not found".format(filepath))

        return filepath

    def rm_objects(self):
        return
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None
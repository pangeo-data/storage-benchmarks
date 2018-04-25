""""
   Set up HDF5 Datasets on various backends

"""

from subprocess import call
from sys import platform
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
_PLATFORM = platform

if _PLATFORM == 'darwin':
    GCSFUSE = '/usr/local/bin/gcsfuse'
else:
    GCSFUSE = '/usr/bin/gcsfuse'


class SingleHDF5POSIXFile(object):
    """
    Single HDF5 POSIX File  

    """

    def __init__(self, backend='POSIX'):
        self.backend = backend
        self.gcs_benchmark_root = getTestConfigValue("gcs_benchmark_root")
        self.gcs_hdf5_fuse      = getTestConfigValue("gcs_hdf5_fuse")
        self.temp_dir = None
        self.suffix = ".h5"

    def get_temp_filepath(self):
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp()
        filename = 'temp-{}{}'.format(next(_counter), self.suffix)

        # If we're using FUSE, we'll use the temp_dir as a mount point and
        # create our test directories and files in there.
        if self.backend == 'FUSE':
            if not self.gcs_hdf5_fuse:
                raise NotImplementedError("Missing config for hdf5 FUSE test.")
            self.dir_store = self.temp_dir + '/%s' % self.gcs_hdf5_fuse
            call([GCSFUSE, self.gcs_benchmark_root, self.temp_dir])
            # Remove previous test runs
            if os.path.exists(self.dir_store):
                shutil.rmtree(self.dir_store)
            os.makedirs(self.dir_store)
            filepath = os.path.join(self.dir_store, filename)
            return filepath
        elif self.backend == 'POSIX':
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
        if self.temp_dir:
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None
        if self.backend == 'FUSE':
            if not self.gcs_hdf5_FUSE or not self.gcp_project_name:
                return
            if platform == 'darwin':
                call(["umount", self.temp_dir])
            elif platform == 'linux':
                call(["fusermount",  "-u", self.temp_dir])

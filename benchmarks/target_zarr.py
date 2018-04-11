""""
   Set up Zarr Datasets on various backends
   TODO: These target libraries could be just single library with a single
   class with options 


"""

from subprocess import call, Popen
from sys import platform
from . import getTestConfigValue
from . import benchmark_tools as bmt
import gcsfs
import os
import tempfile
import itertools
import shutil
import zarr

_counter = itertools.count()
_DATASET_NAME = "default"
_PLATFORM = platform

if _PLATFORM == 'darwin':
    GCSFUSE = '/usr/local/bin/gcsfuse'
else:
    GCSFUSE = '/usr/bin/gcsfuse'

class ZarrStore(object):
    """
    Set up necessary variables and bits to run data operations on a Zarr
    backend. For local filesystems, generally consists of configuring temp
    directories to save datasets while in cloud environments, this will 
    mean connecting and authenticating to resources using native tools.

    Being consistent with rm_objects method is important here as it will 
    prevent clutter of potentially large unwanted datasets persisting in
    random locations following completion of tests.

     Note: Test expects that the following config settings are defined:
    * gcp_ and gcs_ - These tests should NOT run if these variables are
                      not set up correctly.

    - parmams

    storage_obj: Reference object that we read/write data to/from. 
                 For POSIX, this is a directory, and for cloud storage
                 it's the object store.


    """

    def __init__(self, backend='POSIX'):
        # Initialize all values
        self.backend            = backend
        self.gcp_project_name   = getTestConfigValue("gcp_project")
        self.gcs_zarr           = getTestConfigValue("gcs_zarr")
        self.gcs_zarr_FUSE      = getTestConfigValue("gcs_zarr_FUSE")
        self.gcs_benchmark_root = getTestConfigValue("gcs_benchmark_root")
        self.suffix             = ".zarr" 

    def get_temp_filepath(self):
        if self.backend == 'POSIX':
            self.temp_dir    = tempfile.mkdtemp()
            self.storage_obj = os.path.join(self.temp_dir,
                                           'temp-%s%s' % (next(_counter),
                                            self.suffix))
        elif self.backend == 'GCS':
            if not self.gcs_zarr:
                    raise NotImplementedError("Missing config for GCP test")
            
            self.gcp_project = gcsfs.GCSFileSystem(project=self.gcp_project_name, 
                                                   token=None)
            self.storage_obj = gcsfs.mapping.GCSMap(self.gcs_zarr, 
                                                    gcs=self.gcp_project,
                                                    check=True, create=False)
        # GCS FUSE
        elif self.backend == 'FUSE':
            if not self.gcs_zarr_FUSE:
                raise NotImplementedError("Missing config for GCP test")

            self.temp_dir    = tempfile.mkdtemp()
            self.storage_obj = self.temp_dir + "/zarr_FUSE"
            call([GCSFUSE, self.gcs_benchmark_root, self.temp_dir])

            if not os.path.exists(self.storage_obj):
                os.makedirs(self.storage_obj)

        else:
            raise NotImplementedError("Storage backend not implemented.")

    def create_objects(self, dset=None):
        # single Dataset
        if dset == 'xarray':
            self.ds = bmt.rand_xarray()

    def open(self, path, mode):
        return zarr.open(self.storage_obj, mode=mode)

    def save(self, path, data):
        return zarr.save(path, data)


    def rm_objects(self):
        if self.backend == 'POSIX':
            shutil.rmtree(self.storage_obj)

        elif self.backend == 'GCS':
            if not self.gcs_zarr or not self.gcp_project_name:
                return
            gsutil_arg = "gs://%s" % self.gcs_zarr
            Popen(["gsutil", "-q", "-m", "rm", "-r", gsutil_arg])

        elif self.backend == 'FUSE':
            if not self.gcs_zarr_FUSE or not self.gcp_project_name:
                return
            shutil.rmtree(self.storage_obj)
            if platform == 'darwin':
                call(["umount", self.temp_dir])
            elif platform == 'linux':
                call(["fusermount",  "-u", self.temp_dir])
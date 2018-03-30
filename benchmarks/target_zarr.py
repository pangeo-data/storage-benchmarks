""""
   Set up Zarr Datasets on various backends
   TODO: These target libraries could be just single library with a single
   class with options 


"""

from subprocess import call
from . import getTestConfigValue
from . import benchmark_tools as bmt
import gcsfs
import os
import tempfile
import itertools
import shutil

_counter = itertools.count()
_DATASET_NAME = "default"

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


    """

    def __init__(self):
        self.gcp_project_name   = getTestConfigValue("gcp_project")
        self.gcs_zarr           = getTestConfigValue("gcs_zarr")
        self.gcs_zarrfuse       = getTestConfigValue("gcs_zarrfuse")
        self.gcs_benchmark_root = getTestConfigValue("gcs_benchmark_root")

    def create_objects(self, dset=None):
        # single Dataset
        if dset == 'xarray':
            self.ds = bmt.rand_xarray()

    def config_store(self, empty=True, backend='POSIX'):
        """
        Set up the environment for the Zarr store depending on parameters

        backend: [ 'POSIX', 'GGS', 'S3' ]
        empty: whether to populate datastore with synthetic data

        TODO: Checks for proper cloud environmental variables and binaries to 
              be able to actually connect to stuff
        """

        if backend == 'POSIX':
            suffix = '.zarr'
            self.temp_dir = tempfile.mkdtemp()
            self.path = os.path.join(self.temp_dir,
                                     'temp-%s%s' % (next(_counter), suffix))

        elif backend == 'GCS':
            # todo: check for path on this!
            if not self.gcs_zarr:
                raise NotImplementedError("Missing config for GCP test")

            gsutil_arg = "gs://%s" % self.gcs_zarr
            call(["gsutil", "-q", "-m", "rm","-r", gsutil_arg])
            self.gcp_project = gcsfs.GCSFileSystem(project=self.gcp_project_name, 
                                                      token=None)
            self.gcszarr_bucket = gcsfs.mapping.GCSMap('storage-benchmarks/test_zarr/', 
                                                       gcs=self.gcp_project,
                                                       check=True, create=False)

        elif backend =='GCS_FUSE':
            if not self.gcs_zarrfuse:
                raise NotImplementedError("Missing config for GCP test")
            self.temp_dir = tempfile.mkdtemp()
            self.test_dir = self.temp_dir + "/Zarr_FUSE_test"
            call(["gcsfuse", self.gcs_benchmark_root, self.temp_dir])
            if not os.path.exists(self.test_dir):
                os.makedirs(self.test_dir)


    def rm_objects(self, backend='POSIX'):
        if backend == 'POSIX':
            shutil.rmtree(self.temp_dir)

        elif backend == 'GCS':
            if not self.gcs_zarr or not self.gcp_project:
                return
            gsutil_arg = "gs://%s" % self.gcs_zarr
            call(["gsutil", "-q", "-m", "rm", "-r", gsutil_arg])

        elif backend == 'GCS_FUSE':
            if not self.gcs_zarrfuse or not self.gcp_project:
                return
            shutil.rmtree(self.test_dir)
            call(["umount", self.temp_dir])
            shutil.rmtree(self.temp_dir)













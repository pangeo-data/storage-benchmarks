""""
   Set up Zarr Datasets on various backends
   TODO: These target libraries could be just single library with a single
   class with options 


"""

from subprocess import call
from . import randn, randint, requires_dask, create_xarray_random

import gcsfs
import os
import tempfile
import itertools
import shutil
import numpy as np
import pandas as pd
import xarray as xr
import zarr

_counter = itertools.count()
_DATASET_NAME = "default"
_GCS_bucket   = "storage-benchmarks"
_GCS_proj     = "pangeo-181919"
_GCS_zarr     = "%s/test_zarr/" % _GCS_bucket
_GCS_zarr_arg = "gs://%s" % _GCS_zarr
_GCS_zarrfuse = "%s/test_zarr_fuse/" % _GCS_bucket

class ZarrStore(object):
    """
    Set up a Zarr backend.
    TODO: Better docs


    """

    def create_objects(self, dset=None):
        # single Dataset
        if dset == 'xarray':
            self.ds = create_xarray_random()

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
            call(["gsutil", "-q", "-m", "rm","-r", _GCS_zarr_arg])
            self.gcs_proj  = gcsfs.GCSFileSystem(project=_GCS_proj, token=None)
            self.gcs_store = gcsfs.mapping.GCSMap(_GCS_zarr, gcs=self.gcs_proj,
                                                  check=True, create=True)

        elif backend =='GCS_FUSE':
            self.temp_dir = tempfile.mkdtemp()
            self.test_dir = self.temp_dir + "/Zarr_FUSE_test"
            call(["gcsfuse", _GCS_bucket, self.temp_dir])
            if not os.path.exists(self.test_dir):
                os.makedirs(self.test_dir)


    def rm_objects(self, backend='POSIX'):
        if backend == 'POSIX':
            shutil.rmtree(self.temp_dir)

        elif backend == 'GCS':
            gcs_zarr_arg = "gs://%s" % _GCS_zarr
            call(["gsutil", "-q", "-m", "rm", "-r", _GCS_zarr_arg])

        elif backend == 'GCS_FUSE':
            shutil.rmtree(self.test_dir)
            call(["umount", self.temp_dir])
            shutil.rmtree(self.temp_dir)































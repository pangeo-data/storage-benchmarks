""""Configure Zarr datasets on various backends


"""

from . import getTestConfigValue
from . import benchmark_tools as bmt
import gcsfs
import xarray as xr
import zarr

from subprocess import call, Popen
from sys import platform
import os
import tempfile
import itertools
import shutil

_counter = itertools.count()
_DATASET_NAME = "default"
_PLATFORM = platform

if _PLATFORM == 'darwin':
    GCSFUSE = '/usr/local/bin/gcsfuse'
else:
    GCSFUSE = '/usr/bin/gcsfuse'

def get_gcs_root(project):
    """Get object map of a root GCS bucket"""
    fs = gcsfs.GCSFileSystem(project=project, token='cache')
    token = fs.session.credentials
    gcsfs_root = gcsfs.GCSFileSystem(project=project,
                                      token=token)
    return gcsfs_root


class ZarrStore(object):
    """Set up necessary variables and bits to run operations Zarr dataset.

    Being consistent with rm_objects method is important here as it will 
    prevent clutter of potentially large unwanted datasets persisting in
    random locations following completion of tests.

    Note: Undefined expected values skips ASV tests by design.

    Returns:
        storage_obj: Reference object that will run IO operations against.
            For POSIX, this is a directory, and for cloud storage it's an 
            object store.
    """
    def __init__(self, backend='POSIX', dask=False, chunksize=None, shape=None, dtype=None):
        self.backend            = backend
        self.gcp_project_name   = getTestConfigValue("gcp_project")
        self.gcs_zarr           = getTestConfigValue("gcs_zarr")
        self.gcs_zarr_fuse      = getTestConfigValue("gcs_zarr_fuse")
        self.gcs_bucket         = getTestConfigValue("gcs_bucket")
        self.suffix             = ".zarr" 
        self.dask               = dask
        self.shape              = shape
        self.chunksize          = chunksize
        self.dtype              = dtype

    def get_temp_filepath(self):
        if self.backend == 'POSIX':
            self.temp_dir    = tempfile.mkdtemp()
            self.dir_store   = os.path.join(self.temp_dir,
                                            'temp-%s%s' % (next(_counter),
                                            self.suffix))
            # Saving dask objects as Zarr requires more than just a filehandle
            if not self.dask:
                self.storage_obj = self.dir_store
            else:
                self.storage_obj = zarr.create(shape=self.shape, chunks=self.chunksize,
                                               store=self.dir_store, dtype=self.dtype, 
                                               overwrite=True)
        elif self.backend == 'GCS':
            if not self.gcs_zarr:
                    raise NotImplementedError("Missing config for GCP test")
            
            # HACK in order to give worker pods read/write to storage
            fs = gcsfs.GCSFileSystem(project=self.gcp_project_name, token='cache')
            token = fs.session.credentials
            self.gcp_project = gcsfs.GCSFileSystem(project=self.gcp_project_name, 
                                                   token=token)
            self.gcsfsmap    = gcsfs.mapping.GCSMap(self.gcs_zarr, 
                                                    gcs=self.gcp_project,
                                                    check=True, create=False)
            if not self.dask:
                gsutil_arg = "gs://%s" % self.gcs_zarr
                call(["gsutil", "-q", "-m", "rm", "-r", gsutil_arg])
                self.storage_obj = self.gcsfsmap
            else: 
                self.storage_obj = zarr.create(shape=self.shape, chunks=self.chunksize,
                                               store=self.gcsfsmap, dtype=self.dtype, 
                                               overwrite=True)
            
        elif self.backend == 'FUSE':
            if not self.gcs_zarr_fuse:
                raise NotImplementedError("Missing config for FUSE test")

            self.temp_dir    = tempfile.mkdtemp()
            self.dir_store = self.temp_dir + self.gcs_zarr_fuse
            call([GCSFUSE, self.gcs_bucket, self.temp_dir])

            # Remove previous test runs
            if os.path.exists(self.dir_store):
                shutil.rmtree(self.dir_store)
            os.makedirs(self.dir_store)

            # Return the path if this isn't Dask
            # TODO: This should be a function
            if not self.dask:
                self.storage_obj = self.dir_store
            else:
                self.storage_obj = zarr.create(shape=self.shape, chunks=self.chunksize,
                                               store=self.dir_store, dtype=self.dtype, 
                                               overwrite=True)
        else:
            raise NotImplementedError("Storage backend not implemented.")

    def open(self, path, mode):
        return zarr.open(self.storage_obj, mode=mode)

    def open_store(self, directory):
        """Use Xarray to open a dataset"""

        if self.backend == 'GCS':
            self.gcsfs_root = get_gcs_root(self.gcp_project_name)
            self.gcsfsmap   = gcsfs.mapping.GCSMap('pangeo-data/storage-benchmarks/%s' %  
                                                    directory, gcs=self.gcsfs_root,
                                                    check=False, create=False)
            return xr.open_zarr(self.gcsfsmap)
        elif self.backend == 'FUSE':
            #self.temp_dir    = tempfile.mkdtemp()
            #call([GCSFUSE, self.gcs_bucket, self.temp_dir])
            return xr.open_zarr('/gcs/storage-benchmarks/' + directory)

    def save(self, path, data):
        return zarr.save(path, data)


    def rm_objects(self):
        if self.backend == 'POSIX':
            shutil.rmtree(self.dir_store)

        elif self.backend == 'GCS':
            if not self.gcs_zarr or not self.gcp_project_name:
                return
            gsutil_arg = "gs://%s" % self.gcs_zarr
            Popen(["gsutil", "-q", "-m", "rm", "-r", gsutil_arg])

        elif self.backend == 'FUSE':
            if not self.gcs_zarr_fuse or not self.gcp_project_name:
                return
            shutil.rmtree(self.dir_store)
            if platform == 'darwin':
                call(["umount", self.temp_dir])
            elif platform == 'linux':
                call(["fusermount",  "-u", self.temp_dir])

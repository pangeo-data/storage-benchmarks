""""
   Set up HDF5 Datasets on various backends
"""

import os
import itertools
import numpy as np
import h5pyd
import time
from . import getTestConfigValue

_counter = itertools.count()
_DATASET_NAME = "default"
      
_LOCA_PATH = "hdf5://nex/loca/ACCESS1-0/16th/historical/r1i1p1/tasmax/tasmax_day_ACCESS1-0_historical_r1i1p1_19500101-19501231.LOCA_2016-04-02.16th.nc"
class SingleHDF5HSDSFile(object):
    """
    Single HSDS File (domain)  

    Note: Test expects that the following config settings are defined:
    * hs_username - HSDS username that test will run under
    * hs_password - HSDS password for given username
    * hs_endpoint - HSDS http endpoint

    In addition the asvtest folder needs to have been created.  
    This can be done with the hstouch tool:
       $hstouch /home/${HS_USERNAME}/asvtest/

    """   
    def __init__(self):
        self.username = getTestConfigValue("hs_username")
        self.password = getTestConfigValue("hs_password")
        self.endpoint = getTestConfigValue("hs_endpoint")
        if self.username:
            home_folder = os.path.join("/home", self.username)  
            self.temp_dir = os.path.join(home_folder, "asvtest/")   
            suffix = '.h5'
            self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))

    def open(self, path, mode):
        return h5pyd.File(path, mode, endpoint=self.endpoint, username=self.username, password=self.password)

    def create_objects(self, empty=True):   
        print("create object:", self.path)
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

    def get_tasmax_file(self, year=1950):
        f = self.open(_LOCA_PATH, 'r')
        return f

    def rm_objects(self):
        if not self.username or not self.password or not self.endpoint:
            return
        folder = h5pyd.Folder(self.temp_dir, mode='a', endpoint=self.endpoint, username=self.username, password=self.password)
        if len(folder) == 0:
            time.sleep(10)  # allow time for HSDS to sync to S3
        for name in folder:
            del folder[name]

 


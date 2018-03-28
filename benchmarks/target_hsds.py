""""
   Set up HDF5 Datasets on various backends
"""

import os
import itertools
import numpy as np
import h5pyd
import time
from . import getTestConfigValue, create_numpy_random

_counter = itertools.count()
_DATASET_NAME = "default"
      

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

    def create_objects(self, empty=True):   
        self.h5file = h5pyd.File(self.path, 'w', endpoint=self.endpoint, username=self.username, password=self.password)
        data = create_numpy_random(self.nz)
        dset = self.h5file.create_dataset(_DATASET_NAME, (self.nz,self.ny,self.nx), dtype = self.dtype)
        self.dset_name = _DATASET_NAME
        # Writing the entire dataset in one h5pyd call is not yet supported for large datasets, so write in slices
        if not empty:
            for i in range(self.nz):
                dset[i, :, :] = data[i, :, :]

        self.h5file.close()

    def rm_objects(self):
        if not self.username or not self.password or not self.endpoint:
            return
        folder = h5pyd.Folder(self.temp_dir, mode='a', endpoint=self.endpoint, username=self.username, password=self.password)
        if len(folder) == 0:
            time.sleep(10)  # allow time for HSDS to sync to S3
        for name in folder:
            del folder[name]

 


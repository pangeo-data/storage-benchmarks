""""
   Set up HDF5 Datasets on various backends
"""

import os
import itertools
import numpy as np
import h5pyd
import time

_counter = itertools.count()
_DATASET_NAME = "default"
 
     

class SingleHDF5HSDSFile(object):
    """
    Single HSDS File (domain)  

    Note: Test expects that the following environment variables are defined:
    * HS_USERNAME - HSDS username that test will run under
    * HS_PASSWORD - HSDS password for given username
    * HS_ENDPOINT - HSDS http endpoint

    In addition the asvtest folder needs to have been created.  
    This can be done with the hstouch tool:
       $hstouch /home/${HS_USERNAME}/asvtest/

    """       
    def create_objects(self, empty=True):
        suffix = '.h5'
        # HS_USERNAME (HSDS user accont name) environment is required
        home_folder = os.path.join("/home", os.getenv("HS_USERNAME"))  
        self.temp_dir = os.path.join(home_folder, "asvtest/")   
        self.path = os.path.join(self.temp_dir,
                                 'temp-%s%s' % (next(_counter), suffix))
        self.h5file = h5pyd.File(self.path, 'w')

        if not empty:
            self.nz = 1024
            self.ny = 256
            self.nx = 512
            self.shape = (self.nz, self.ny, self.nx)
            self.dtype = 'f8'
            # Create a 1GB dataset
            data = np.random.rand(*self.shape).astype(self.dtype)
            # self.h5file.create_dataset(_DATASET_NAME, data=data)  # this fails because it's trying to write too much data at once
            dset = self.h5file.create_dataset(_DATASET_NAME, (self.nz,self.ny,self.nx), dtype = self.dtype)
             
            # Writing the entire 1GB in one h5pyd call is not yet supported, so write in slices
            for i in range(self.nz):
                dset[i, :, :] = data[i, :, :]

            self.h5file.close()

    def rm_objects(self):
        folder = h5pyd.Folder(self.temp_dir, mode='a')
        if len(folder) == 0:
            time.sleep(10)  # allow time for HSDS to sync to S3
        for name in folder:
            del folder[name]

 


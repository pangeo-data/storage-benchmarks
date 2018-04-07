""""
   Set up HDF5 Datasets on various backends
"""

import os
import itertools
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
        self.temp_dir = None
        self.suffix = ".h5"

    def get_temp_filepath(self):
        if not self.temp_dir:
            if not self.username:
                raise NotImplementedError("Username not set")
            home_folder = os.path.join("/home", self.username)  
            self.temp_dir = os.path.join(home_folder, "asvtest/")  
        filename = 'temp-{}{}'.format(next(_counter), self.suffix)
        return os.path.join(self.temp_dir, filename)
                               
    def open(self, path, mode):
        return h5pyd.File(path, mode, endpoint=self.endpoint, username=self.username, password=self.password)

    def get_tasmax_filepath(self, year=1950):
        if not self.username or not self.password or not self.endpoint:
            raise NotImplementedError("Missing config for HSDS tests")
        # TBD: Get different files for different years
        filepath = _LOCA_PATH
        try:
            self.open(filepath, 'r')
        except IOError:
            raise NotImplementedError("Domain: {} not found".format(filepath))

        return filepath

    def rm_objects(self):
        if not self.username or not self.password or not self.endpoint:
            return
        if not self.temp_dir:
            return
        folder = h5pyd.Folder(self.temp_dir, mode='a', endpoint=self.endpoint, username=self.username, password=self.password)
        if len(folder) == 0:
            time.sleep(10)  # allow time for HSDS to sync to 
        for name in folder:
            del folder[name]
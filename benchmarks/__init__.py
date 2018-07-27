from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import itertools
import yaml
import os

_counter = itertools.count()
_CONFIG_FILE = "test-kai.conf.yaml"


def parameterized(names, params):
    def decorator(func):
        func.param_names = names
        func.params = params
        return func
    return decorator


def requires_dask():
    try:
        import dask  # noqa
    except ImportError:
        raise NotImplementedError


def getTestConfigValue(k):
    val = None
    # check to see if we have an environment override
    if k.upper() in os.environ:
        val = os.environ[k.upper()]
    else:
        cwd = os.getenv("PWD")  # not the same as os.getcwd()
        config_file_path = os.path.join(cwd, _CONFIG_FILE)
        with open(config_file_path, "r") as f:
            cfg = yaml.load(f)
        try:
            val = cfg[k]
        #if k in cfg:
        #    val = cfg[k]
        except ValueError:
            print("Value for %k is undefined or not found in config file." % k)
    return val

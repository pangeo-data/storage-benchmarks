# storage-benchmarks
Modified and somewhat abused ASV suite of tests for running storage IO benchmarks against
Pangeo environments.

[airspeedvelocity](http://asv.readthedocs.io/en/latest) is the basis of these benchmarks.


## Basics and running the benchmarks.
You typically run ASV benchmarks through the command line, but with this implementation, this is done through a Python script:

```
usage: run_benchmarks.py [-h] -b BENCHMARK [BENCHMARK ...]
                         [-n N_RUNS [N_RUNS ...]]
```

For example, if you want to run all the GCP Kubernetes tests 6 times, you'd execute,

```
python run_benchmarks.py -b gcp_kubernetes -n 6
```

This will then generate all the benchmark runs, and scrape the resultant JSON output and append them to a CSV file.

## Suite of Tests

For all use cases, the following perfomance tests are conducted:

* IO
	* Copy file from cloud to local
	* Write to cloud storage
	* Delete file
* Get time series at a single point
* Aggregations
	* global mean / variance of SST as a function of time
 	* mean / variance over longitude and time (i.e. zonal mean)
	* monthly climatology
	* convolution (smoothing) over spatial dimensions
	* multidimensional Fourier transforms

## Storage/Backend/API Combinations

1. netcdf -> POSIX -> local storage
1. netcdf -> POSIX -> some sort of disk presentation layer (e.g. fuse) -> cloud bucket
1. Zarr -> POSIX -> local storage
1. Zarr -> cloud bucket
1. h5netcdf -> POSIX -> local storage
1. h5netcdf -> hsds -> cloud bucket
1. TileDB -> cloud (currently only S3)
1. TileDB -> POSIX (local, Lustre, etc)
1. TileDB -> HDFS

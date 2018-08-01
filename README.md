# storage-benchmarks
Modified ASV suite of benchmark tests to gather IO performance metrics in Pangeo environments. Set of tests exist for cloud, HPC, and workstation-like environments across different mixture of storage backends and APIs. We're mainly concerned with benchmarking Xarray/Dask performance in both single and multiprocessor/multithreaded/clustered environments.

[airspeedvelocity](http://asv.readthedocs.io/en/latest) is the basis of these benchmarks, although workflow has been modified to accomodate gathering IO statistics.

## Basics and running the benchmarks.

You typically run ASV benchmarks through its command line tool, but with this implementation, the runs are conducted through a Python script:

```
usage: run_benchmarks.py [-h] -b BENCHMARK [BENCHMARK ...]
                         [-n N_RUNS [N_RUNS ...]]

```

Where `BENCHMARK` is a regex of the benchmark test you'd like to run. For example, if you want to run all the GCP Kubernetes read tests 10 times, you'd execute,

```
python run_benchmarks.py -b gcp_kubernetes_read* -n 10

```

This will then generate all the benchmark runs, and scrape the resultant JSON output and append them to a CSV file. Data is collected from most recent ASV JSON results file for the machine the tests are being run on. If your directory has results from a different machine, this script will not collect data from that at this time. 

## Suite of Tests

The following perfomance tests are conducted:

* IO
	* Read
	* Write
* Aggregations
	* mean / variance
	* convolution (smoothing) over spatial dimensions
	* multidimensional Fourier transforms

## Storage/Backend/API Combinations

1. netcdf -> POSIX -> local storage
1. netcdf -> POSIX -> some sort of disk presentation layer (e.g. FUSE) -> cloud bucket
1. Zarr -> POSIX -> local storage
1. Zarr -> cloud bucket
1. h5netcdf -> POSIX -> local storage
1. h5netcdf -> hsds -> cloud bucket
1. TileDB -> cloud (currently only S3)
1. TileDB -> POSIX (local, Lustre, etc)
1. TileDB -> HDFS

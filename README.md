# storage-benchmarks
testing performance of different storage layers

Uses [airspeedvelocity](http://asv.readthedocs.io/en/latest) to organize benchmarks.

Latest github master of asv is required

    $ pip install git+https://github.com/airspeed-velocity/asv.git

To run the benchmarks (from repo root directory)

    $ asv run

Then, update reports

    $ asv publish

Finally, you can run a local webserver to view results:

    $ asv preview

Browsing to http://127.0.0.1:8080 will show your reports.

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

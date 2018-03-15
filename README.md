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


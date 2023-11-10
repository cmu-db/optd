# optd

A query optimizer. Currently aimed for a cascades optimizer for Apache Arrow Datafusion.

## Structure

* `datafusion-optd-cli`: patched Apache Arrow Datafusion cli that calls into optd
* `datafusion-optd-bridge`: implementation of Apache Arrow Datafusion query planner as a bridge between optd and Apache Arrow Datafusion.
* `optd-core`: the core framework of optd.
* `optd-datafusion-repr`: representation of Apache Arrow Datafusion plan nodes in optd.

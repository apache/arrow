# Overview

Apache Arrow provides a Datasets API, which acts as an abstraction over a collection of files in a
storage backend. The files can be of different formats like IPC, Feather, Parquet, and CSV. Since, in
our SkyhookDM project we pushdown compute operations into the storage layer, we needed to create a file format that allows operating on parts of a file independently. Since, Parquet is a very optimized file format due to its filter and projection capabilities, we decided to use Parquet as the base for our new file format.

# Code Structure

* `arrow/dataset/file_rados_parquet.h`: This file contains the `RadosParquetFileFormat` definitions.
* `arrow/dataset/rados.h`: Contains the interface to the `librados` SDK.
* `arrow/dataset/rados_utils.h`: Contains functions for (de)serializing query options, query results, etc.

# APIs

`RadosParquetFileFormat`: 
`DirectObjectAccrss`:
`RadosCluster`:

# Setting up Development Environment

```bash
git clone https://github.com/uccross/arrow
cd arrow/
git checkout rados-dataset-dev/ 
export UBUNTU=20.04
docker-compose run ubuntu-cpp-cls
docker-compose run ubuntu-python-cls
```

# Getting started with the C++ API

Please look at the C++ end-to-end tests to get familiar with the API.

# Getting started with the Python API

```python
import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow.dataset import RadosParquetFileFormat as RPFFormat

dataset = ds.dataset("file:///mnt/cephs/nyc", format=RPFFormat(b"/etc/ceph/ceph.conf"))

# get the whole table
dataset.to_table()

# apply projection
dataset.to_table(columns=["fare_amount", "VendorID"])

# apply filtering
dataset.to_table(columns=["fare_amount", "VendorID"], filter=(ds.field("fare_amount") > 100.0)
```
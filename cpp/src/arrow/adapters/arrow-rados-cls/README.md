# SkyhookDM-Arrow

Apache Arrow provides a `Dataset` API, which acts as an abstraction over a collection of files in different storage backend like S3 and HDFS. It supports different file formats like CSV and Parquet through the `FileFormat` API. In SkyhookDM, since we require to pushdown
compute operations into the Storage backend, we created a new file format on top of Parquet, namely a `RadosParquetFileFormat` which besides providing the benefits of Parquet, allows pushing down filter and projection operations into the storage backend to minimize data moved through the network.

# Installation

* For installing SkyhookDM with Rook, check out [this](https://github.com/JayjeetAtGithub/skyhookdm-arrow-docker/blob/master/README.md#deploying-skyhookdm-arrow-on-a-rook-cluster) guide.

* TODO: add instructions for deploying on Cloudlab.

# Getting Started

TODO: create a reproducible getting started

```python
import pyarrow as pa
import pyarrow.dataset as ds

dataset = ds.dataset("file:///mnt/cephs/nyc", format=ds.RadosParquetFileFormat(b"/etc/ceph/ceph.conf"))

# fetch the whole table
dataset.to_table()

# apply projection
dataset.to_table(columns=["fare_amount", "VendorID"])

# now also apply filtering
dataset.to_table(columns=["fare_amount", "VendorID"], filter=(ds.field("fare_amount") > 100.0)
```

# Features

* Enables pushing down filters, projections, compute operations to the Storage backend for minimal data transfer over the network.

* Allows storing data in Parquet files for minimizing Disk I/O though predicate and projection pushdown.

* Plugs-in seamlessly into the Arrow Dataset API and leverages all its functionality like dataset discovering,  partition pruning, etc.

* Minimal overhead in requirements: 
    1) Requires CephFS to be mounted. 
    2) Requires using the `SplittedParquetWriter` API to write arrow Tables.

* Built on top of Ceph v15.2.x.

# Code Structure

### Client side - C++

* `arrow/dataset/file_rados_parquet.h`: This file contains the definitions of 3 APIs. The `RadosCluster` , `DirectObjectAccess`, and the `RadosParquetFileFormat`. The `RadosCluster` API helps create a connection to the Ceph cluster and provides a handle to the cluster that can be passed around. The `DirectObjectAccess` API provides abstractions for converting filenames in CephFS to object IDs in the Object store and allows interacting with the objects directly. The `RadosParquetFileFormat` API takes in the direct object access construct as input and contains the logic of pushing down scans to the underlying objects that make up a file.

* `arrow/dataset/rados.h`: Contains a wrapper for the `librados` SDK for exposing `librados` methods like `init2`, `connect`, `stat`, `ioctx_create`, and `exec` which are required for establishing the connection to the Ceph cluster and for operating on objects directly. 

* `arrow/dataset/rados_utils.h`: Contains utility functions for (de)serializing query options, query results, etc. Currently, we serialize all the expressions and schemas into a `ceph::bufferlist`, but in a later release, we plan to use a Flatbuffer schema for making the scan options more scalable.

### Client side - Python

TODO

### Storage side

* `arrow/adapters/arrow-rados-cls/cls_arrow.cc`: Contains the Rados objclass functions and APIs for interacting with objects in the OSDs. Also, it includes a `RandomAccessObject` API to give a random access file view of objects for allowing operations like reading byte ranges, seeks, tell, etc. 

# Setting up the development environment

**NOTE:** Please make sure docker and docker-compose is installed.

1. Clone the repository and checkout into the development branch.

```bash
git clone https://github.com/uccross/arrow
cd arrow/
git checkout rados-dataset-dev/ 
```

2. Build and test the C++ client.
```
export UBUNTU=20.04
docker-compose run ubuntu-cpp-cls.
```

3. Build and test the Python client.
```
export UBUNTU=20.04
docker-compose run ubuntu-python-cls.
```

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# <img src="https://iris-hep.org/assets/logos/skyhookdmLogoJeff.png" width="64" valign="middle" alt="Skyhook"/> SkyhookDM-Arrow

Apache Arrow provides a [`Dataset`](https://arrow.apache.org/docs/cpp/api/dataset.html) API, which acts as an abstraction over a collection of files in different storage backend like S3 and HDFS. It supports different file formats like CSV and Parquet through the [`FileFormat`](https://arrow.apache.org/docs/cpp/api/dataset.html#_CPPv4N5arrow7dataset10FileFormatE) API. In SkyhookDM, since we require to pushdown
compute operations into the Storage backend, we created a new file format on top of Parquet, namely a `RadosParquetFileFormat` which besides providing the benefits of Parquet, allows pushing down filter and projection operations into the storage backend to minimize data moved through the network.

# Getting Started

**NOTE:** Please make sure [docker](https://docs.docker.com/engine/install/ubuntu/) and [docker-compose](https://docs.docker.com/compose/install/) is installed.

* Clone the repository.
```bash
git clone --branch v0.1.1 https://github.com/uccross/arrow
```

* Run the `ubuntu-cls-demo` step in the docker-compose file. This step will start a single node Ceph cluster inside the container, mount CephFS, put sample data into CephFS, and open an example Jupyter notebook with PyArrow installed.
```bash
cd arrow/
docker-compose run --service-ports ubuntu-cls-demo
```

# Installation Instructions

* For installing SkyhookDM-Arrow with [Rook](https://rook.io) on Kubernetes, check out [this](https://github.com/uccross/skyhookdm-arrow-docker/blob/master/README.md#deploying-skyhookdm-arrow-on-a-rook-cluster) guide.

* For installing SkyhookDM-Arrow on CloudLab, check out [this](https://github.com/uccross/skyhookdm-workflows/tree/master/cloudlab#deploy-ceph-skyhookdm-on-cloudlab) guide. To deploy SkyhookDM on bare-metal in general, check out [this](docs/deploy.md) guide.

# Salient Features

* Enables pushing down filters, projections, compute operations to the Storage backend for minimal data transfer over the network.

* Allows storing data in Parquet files for minimizing Disk I/O though predicate and projection pushdown.

* Plugs-in seamlessly into the Arrow Dataset API and leverages all its functionality like dataset discovering,  partition pruning, etc.

* Minimal overhead in requirements: 
    1) Requires CephFS to be mounted. 
    2) Requires using the `SplittedParquetWriter` API to write arrow Tables.

* Built on top of latest Ceph v15.2.x.

# Code Structure

### Client side - C++

* [`file_rados_parquet.h`](../../dataset/file_rados_parquet.h): This file contains the definitions of 3 APIs. The `RadosCluster` , `DirectObjectAccess`, and the `RadosParquetFileFormat`. The `RadosCluster` API helps create a connection to the Ceph cluster and provides a handle to the cluster that can be passed around. The `DirectObjectAccess` API provides abstractions for converting filenames in CephFS to object IDs in the Object store and allows interacting with the objects directly. The `RadosParquetFileFormat` API takes in the direct object access construct as input and contains the logic of pushing down scans to the underlying objects that make up a file.

* [`rados.h`](../../dataset/rados.h): Contains a wrapper for the `librados` SDK for exposing `librados` methods like `init2`, `connect`, `stat`, `ioctx_create`, and `exec` which are required for establishing the connection to the Ceph cluster and for operating on objects directly. 

* [`rados_utils.h`](../../dataset/rados_utils.h): Contains utility functions for (de)serializing query options, query results, etc. Currently, we serialize all the expressions and schemas into a `ceph::bufferlist`, but in a later release, we plan to use a Flatbuffer schema for making the scan options more scalable.

### Client side - Python

* [`_rados.pyx`](../../../../../python/pyarrow/_rados.pyx): Contains Cython bindings to the `RadosParquetFileFormat` C++ API.

* [`rados.py`](../../../../../python/pyarrow/rados.py): This file contains the definition of the `SplittedParquetWriter`. It is completely implemented in Python.

### Storage side

* [`cls_arrow.cc`](./cls_arrow.cc): Contains the Rados objclass functions and APIs for interacting with objects in the OSDs. Also, it includes a `RandomAccessObject` API to give a random access file view of objects for allowing operations like reading byte ranges, seeks, tell, etc. 

# Development

Check out these [instructions](docs/contributing.md) for setting up a local development environment.

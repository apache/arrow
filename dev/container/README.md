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

# Apache Arrow development container
* Includes all dependencies for Arrow development
* Builds are incremental,  mirrored to local file system
* Resolves [ARROW-2486](https://issues.apache.org/jira/browse/ARROW-2486)

## Get started

### [Install Docker](https://docs.docker.com/install/)

### Acquire image

```
$ docker pull quiltdata/arrow
```

### Populate host directory
Keep git repos and subsequent build products in a persistent local
directory, `/io`.

```
$ mkdir -p io/arrow
$ git clone https://github.com/apache/arrow.git io/arrow
$ mkdir -p io/parquet-cpp
$ git clone https://github.com/apache/parquet-cpp.git io/parquet-cpp
```
Alternatively, if you wish to use existing git repos, you can nest them
under `/io`.

### Run container, mount `/io` as volume

```
$ docker run \
	--shm-size=2g \
	-v /LOCAL/PATH/TO/io:/io \
	-it quiltdata/arrow
```

### Use container
Run scripts to build executables.

See also [Arrow dev docs](https://arrow.apache.org/docs/python/development.html).

```
$ source script/env.sh
$ script/arrow-build.sh
$ script/parquet-build.sh
$ script/pyarrow-build.sh
# run tests
$ cd /io/arrow/python
$ py.test pyarrow
```

## Build container

```
$ docker build -t USERNAME/arrow .
```

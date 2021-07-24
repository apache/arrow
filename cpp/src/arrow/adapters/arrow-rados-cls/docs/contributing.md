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

# Setting up the development environment

## Using docker

1. Clone the SkyhookDM repository.
```bash
git clone https://github.com/uccross/skyhookdm-arrow
cd skyhookdm-arrow/
```

2. Run the SkyhookDM container in interactive mode.
```bash
docker run -it -v $PWD:/w -w /w --privileged uccross/skyhookdm-arrow:v0.2.0 bash
```

3. Run the build script.
```bash
./cpp/src/arrow/adapters/arrow-rados-cls/scripts/build.sh
```

4. Run the tests script.
```bash
./cpp/src/arrow/adapters/arrow-rados-cls/scripts/test.sh
```

## Using Archery

**NOTE:** Please make sure [docker](https://docs.docker.com/engine/install/ubuntu/) and [docker-compose](https://docs.docker.com/compose/install/) is installed.

1. Clone the repository.
```bash
git clone --branch rados-dataset-dev https://github.com/uccross/arrow
```

2. Install [Archery](https://arrow.apache.org/docs/developers/archery.html#), the daily development tool by Apache Arrow community.
```bash
cd arrow/
pip install -e dev/archery
```

2. Build and test the C++ client.
```bash
export UBUNTU=20.04
archery docker run ubuntu-cpp-cls
```

3. Build and test the Python client.
```bash
export UBUNTU=20.04
archery docker run ubuntu-python-cls
```

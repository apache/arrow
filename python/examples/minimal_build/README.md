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

# Minimal Python source build on Linux

This directory shows how to bootstrap a local build from source on Linux with
an eye toward maximum portability across different Linux distributions. This
may help for contributors debugging build issues caused by their local
environments.

## Fedora 31

Build image:

```
docker build -t arrow_fedora_minimal -f Dockerfile.fedora
```

Build with conda or pip/virtualenv:

```
# With pip/virtualenv
docker run --rm -t -i -v $PWD:/io arrow_fedora_minimal /io/build_venv.sh

# With conda
docker run --rm -t -i -v $PWD:/io arrow_fedora_minimal /io/build_conda.sh
```

## Ubuntu 18.04

Build image:

```
docker build -t arrow_ubuntu_minimal -f Dockerfile.ubuntu
```

Build with conda or pip/virtualenv:

```
# With pip/virtualenv
docker run --rm -t -i -v $PWD:/io arrow_ubuntu_minimal /io/build_venv.sh

# With conda
docker run --rm -t -i -v $PWD:/io arrow_ubuntu_minimal /io/build_conda.sh
```

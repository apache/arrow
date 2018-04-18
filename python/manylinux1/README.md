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

## Manylinux1 wheels for Apache Arrow

This folder provides base Docker images and an infrastructure to build
`manylinux1` compatible Python wheels that should be installable on all
Linux distributions published in last four years.

The process is split up in two parts: There are base Docker images that build
the native, Python-indenpendent dependencies. For these you can select if you
want to also build the dependencies used for the Parquet support. Depending on
these images, there is also a bash script that will build the pyarrow wheels
for all supported Python versions and place them in the `dist` folder.

### Build instructions

```bash
# Create a clean copy of the arrow source tree
git clone ../../ arrow
# Build the native baseimage
docker build -t arrow-base-x86_64 -f Dockerfile-x86_64 .
# Build the python packages
docker run --shm-size=2g --rm -t -i -v $PWD:/io arrow-base-x86_64 /io/build_arrow.sh
# Now the new packages are located in the dist/ folder
ls -l dist/
```

### Updating the build environment

In addition to the docker images that contains the Arrow C++ and Parquet C++
builds, we also have another base image that only contains their dependencies.
This image is less often updated. In the case we want to update a dependency to
a new version, we also need to adjust it. You can rebuild this image using

```bash
docker build -t arrow_manylinux1_x86_64_base -f Dockerfile-x86_64_base .
```

For each dependency, we have a bash script in the directory `scripts/` that
downloads the sources, builds and installs them. At the end of each dependency
build the sources are removed again so that only the binary installation of a
dependency is persisted in the docker image. When you do local adjustments to
this image, you need to change the `FROM` line in `Dockerfile-x86_64` to pick up
the new image:

```
FROM arrow_manylinux1_x86_64_base
```

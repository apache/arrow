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

You can build the wheels with the following
command (this is for Python 2.7 with unicode width 16, similarly you can pass
in `PYTHON_VERSION="3.5"`, `PYTHON_VERSION="3.6"` or `PYTHON_VERSION="3.7"` or
use `PYTHON_VERSION="2.7"` with `UNICODE_WIDTH=32`):

```bash
# Build the python packages
docker run --env PYTHON_VERSION="2.7" --env UNICODE_WIDTH=16 --shm-size=2g --rm -t -i -v $PWD:/io -v $PWD/../../:/arrow quay.io/ursa-labs/arrow_manylinux1_x86_64_base:latest /io/build_arrow.sh
# Now the new packages are located in the dist/ folder
ls -l dist/
```

### Updating the build environment
The base docker image is less often updated. In the case we want to update
a dependency to a new version, we also need to adjust it. You can rebuild
this image using

```bash
docker build -t arrow_manylinux1_x86_64_base -f Dockerfile-x86_64_base .
```

For each dependency, we have a bash script in the directory `scripts/` that
downloads the sources, builds and installs them. At the end of each dependency
build the sources are removed again so that only the binary installation of a
dependency is persisted in the docker image. When you do local adjustments to
this image, you need to change the name of the docker image in the `docker run`
command.

### Using quay.io to trigger and build the docker image

1.  Make the change in the build scripts (eg. to modify the boost build, update `scripts/boost.sh`).

2.  Setup an account on quay.io and link to your GitHub account

3.  In quay.io,  Add a new repository using :

    1.  Link to GitHub repository push
    2.  Trigger build on changes to a specific branch (eg. myquay) of the repo (eg. `pravindra/arrow`)
    3.  Set Dockerfile location to `/python/manylinux1/Dockerfile-x86_64_base`
    4.  Set Context location to `/python/manylinux1`

4.  Push change (in step 1) to the branch specified in step 3.ii

    *  This should trigger a build in quay.io, the build takes about 2 hrs to finish.

5.  Add a tag `latest` to the build after step 4 finishes, save the build ID (eg. `quay.io/pravindra/arrow_manylinux1_x86_64_base:latest`)

6.  In your arrow PR,

    *  include the change from 1.
    *  modify `travis_script_manylinux.sh` to switch to the location from step 5 for the docker image.

## TensorFlow compatible wheels for Arrow

As TensorFlow is not compatible with the manylinux1 standard, the above
wheels can cause segfaults if they are used together with the TensorFlow wheels
from https://www.tensorflow.org/install/pip. We do not recommend using
TensorFlow wheels with pyarrow manylinux1 wheels until these incompatibilities
are addressed by the TensorFlow team [1]. For most end-users, the recommended
way to use Arrow together with TensorFlow is through conda.
If this is not an option for you, there is also a way to produce TensorFlow
compatible Arrow wheels that however do not conform to the manylinux1 standard
and are not officially supported by the Arrow community.

Similar to the manylinux1 wheels, there is a base image that can be built with

```bash
docker build -t arrow_linux_x86_64_base -f Dockerfile-x86_64_ubuntu .
```

Once the image has been built, you can then build the wheels with the following
command (this is for Python 2.7 with unicode width 16, similarly you can pass
in `PYTHON_VERSION="3.5"`, `PYTHON_VERSION="3.6"` or `PYTHON_VERSION="3.7"` or
use `PYTHON_VERSION="2.7"` with `UNICODE_WIDTH=32`)

```bash
# Build the python packages
sudo docker run --env UBUNTU_WHEELS=1 --env PYTHON_VERSION="2.7" --env UNICODE_WIDTH=16 --rm -t -i -v $PWD:/io -v $PWD/../../:/arrow arrow_linux_x86_64_base:latest /io/build_arrow.sh
# Now the new packages are located in the dist/ folder
ls -l dist/
echo "Please note that these wheels are not manylinux1 compliant"
```

[1] https://groups.google.com/a/tensorflow.org/d/topic/developers/TMqRaT-H2bI/discussion

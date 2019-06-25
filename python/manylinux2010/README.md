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

## Manylinux2010 wheels for Apache Arrow

This folder provides base Docker images and an infrastructure to build
`manylinux2010` compatible Python wheels that should be installable on all
Linux distributions published in last four years.

The process is split up in two parts:

1. There are base Docker images that contain the build dependencies for
   Arrow.  Those images do not need to be rebuilt frequently, and are hosted
   on the public quay.io service.

2. Based on on these images, there is a bash script (`build_arrow.sh`) that
   the PyArrow wheels for all supported Python versions, and place them
   in the `dist` folder.

### Building PyArrow

You can build the PyArrow wheels by running the following command in this
directory (this is for Python 2.7 with unicode width 16, similarly you can pass
in `PYTHON_VERSION="3.5"`, `PYTHON_VERSION="3.6"` or `PYTHON_VERSION="3.7"` or
use `PYTHON_VERSION="2.7"` with `UNICODE_WIDTH=32`):

```bash
# Build the python packages
docker run --env PYTHON_VERSION="2.7" --env UNICODE_WIDTH=16 --shm-size=2g --rm -t -i -v $PWD:/io -v $PWD/../../:/arrow quay.io/ursa-labs/arrow_manylinux2010_x86_64_base:latest /io/build_arrow.sh
# Now the new packages are located in the dist/ folder
ls -l dist/
```

### Re-building the build image

In case you want to make changes to the base Docker image (for example because
you want to update a dependency to a new version), you must re-build it.
The Docker configuration is in `Dockerfile-x86_64_base`, and it calls into
scripts stored under the `scripts` directory.

```bash
docker build -t arrow_manylinux2010_x86_64_base -f Dockerfile-x86_64_base .
```

For each dependency, a bash script in the `scripts/` directory downloads the
source code, builds it and installs the build results.  At the end of each
dependency build the sources are removed again so that only the binary
installation of a dependency is persisted in the Docker image.

### Publishing a new build image

If you have write access to the `quay.io` Ursa Labs account, you can directly
publish a build image that you built locally.

For that you need to first tag your image for quay.io upload:
```bash
$ docker image tag arrow_manylinux2010_x86_64_base:latest quay.io/ursa-labs/arrow_manylinux2010_x86_64_base
```

Then you can push it:
```bash
$ docker image push quay.io/ursa-labs/arrow_manylinux2010_x86_64_base
The push refers to repository [quay.io/ursa-labs/arrow_manylinux2010_x86_64_base]
a1ab88d27acc: Pushing [==============>                                    ]  492.5MB/1.645GB
[... etc. ...]
```

### Using quay.io to trigger and build the docker image

You can also create your own `quay.io` repository and trigger builds there from
your Github fork of the Arrow repository.

1.  Make the change in the build scripts (eg. to modify the boost build, update `scripts/boost.sh`).

2.  Setup an account on quay.io and link to your GitHub account

3.  In quay.io,  Add a new repository using :

    1.  Link to GitHub repository push
    2.  Trigger build on changes to a specific branch (eg. myquay) of the repo (eg. `pravindra/arrow`)
    3.  Set Dockerfile location to `/python/manylinux2010/Dockerfile-x86_64_base`
    4.  Set Context location to `/python/manylinux2010`

4.  Push change (in step 1) to the branch specified in step 3.ii

    *  This should trigger a build in quay.io, the build takes about 2 hrs to finish.

5.  Add a tag `latest` to the build after step 4 finishes, save the build ID (eg. `quay.io/pravindra/arrow_manylinux2010_x86_64_base:latest`)

6.  In your arrow PR,

    *  include the change from 1.
    *  modify the `python-manylinux2010` entry in `docker-compose.yml`
       to switch to the location from step 5 for the docker image.

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
### How to build rust's docker image

To build the docker image in development, use

```
docker build -f docker/rust.dockerfile -t ballistacompute/ballista-rust:latest .
```

This uses a multi-stage build, on which the build stage is called `builder`.
Our github has this target cached, that we use to speed-up the build time:

```
export BUILDER_IMAGE=docker.pkg.github.com/ballista-compute/ballista/ballista-rust-builder:main

docker login docker.pkg.github.com -u ... -p ...  # a personal access token to read from the read:packages
docker pull $BUILDER_IMAGE

docker build --cache-from $BUILDER_IMAGE -f docker/rust.dockerfile -t ballista:latest .
```

will build the image by re-using a cached image.

### Docker images for development

This project often requires testing on kubernetes. For this reason, we have a github workflow to push images to 
github's registry, both from this repo and its forks.

The basic principle is that every push to a git reference builds and publishes a docker image.
Specifically, given a branch or tag `${REF}`,

* `docker.pkg.github.com/ballista-compute/ballista/ballista-rust:${REF}` is the latest image from $REF
* `docker.pkg.github.com/${USER}/ballista/ballista-rust:${REF}` is the latest image from $REF on your fork

To pull them from a kubernetes cluster or your computer, you need to have a personal access token with scope `read:packages`,
and login to the registry `docker.pkg.github.com`.

The builder image - the large image with all the cargo caches - is available on the same registry as described above, and is also
available in all forks and for all references.

Please refer to the [rust workflow](.github/workflows/rust.yaml) and [rust dockerfile](docker/rust.dockerfile) for details on how we build and publish these images.

### Get the binary

If you do not aim to run this in docker but any linux-based machine, you can get the latest binary from a docker image on the registry: the binary is statically linked and thus runs on any linux-based machine. You can get it using

```
id=$(docker create $BUILDER_IMAGE) && docker cp $id:/executor executor && docker rm -v $id
```

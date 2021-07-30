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
# Benchmark Builds Env and Hooks
This directory contains: 
- [benchmarks.env](benchmarks.env) - list of env vars used for building Arrow C++/Python/R/Java/JavaScript and running benchmarks using [conbench](https://ursalabs.org/blog/announcing-conbench/).
- [hooks.sh](hooks.sh) - hooks used by <b>@ursabot</b> benchmark builds that are triggered by `@ursabot please benchmark` PR comments. 

## How to add or update Arrow build and run env vars used by `@ursabot` benchmark builds
1. Create `apache/arrow` PR
2. Update or add env var value in [benchmarks.env](../../dev/conbench_envs/benchmarks.env)
3. Add `@ursabot please benchmark` comment to PR
4. Once benchmark builds are done, benchmark results can be viewed via compare/runs links in the PR comment where
- baseline = PR base HEAD commit with unaltered `/dev/conbench_envs/benchmarks.env`
- contender = PR branch HEAD commit with overridden `/dev/conbench_envs/benchmarks.env`

## Why do`@ursabot` benchmark builds need `hooks.sh`?
`@ursabot` benchmark builds are maintained in Ursa's private repo.
Benchmark builds use `hooks.sh` functions as hooks to create conda env with Arrow dependencies and build Arrow C++/Python/R/Java/JavaScript from source for a specific Arrow repo's commit.

Defining hooks in Arrow repo allows benchmark builds for a specific commit to be
compatible with the files/scripts *in that commit* which are used for installing Arrow
dependencies and building Arrow. This allows Arrow contributors to asses the perfomance
implications of different build options, dependency versions, etc by updating
`hooks.sh`.

## Can other repos and services use `benchmarks.env` and `hooks.sh`?

Yes, other repos and services are welcome to use `benchmarks.env` and `hooks.sh` as long as 
- existing hooks are not removed or renamed.
- function definitions for exiting hooks can only be updated in the Arrow commit where Arrow build scripts or files with dependencies have been renamed, moved or added.
- benchmark builds are run using `@ursabot please benchmark` PR comment to confirm that function definition updates do not break benchmark builds.

## How can other repos and services use `benchmarks.env` and `hooks.sh` to setup benchmark env?
Here are steps how `@ursabot` benchmark builds use `benchmarks.env` and `hooks.sh` to setup benchmarking env on Ubuntu:

### 1. Install Arrow dependencies
    sudo su
    apt-get update -y -q && \
        apt-get install -y -q --no-install-recommends \
            autoconf \
            ca-certificates \
            ccache \
            cmake \
            g++ \
            gcc \
            gdb \
            git \
            libbenchmark-dev \
            libboost-filesystem-dev \
            libboost-regex-dev \
            libboost-system-dev \
            libbrotli-dev \
            libbz2-dev \
            libgflags-dev \
            libcurl4-openssl-dev \
            libgoogle-glog-dev \
            liblz4-dev \
            libprotobuf-dev \
            libprotoc-dev \
            libre2-dev \
            libsnappy-dev \
            libssl-dev \
            libthrift-dev \
            libutf8proc-dev \
            libzstd-dev \
            make \
            ninja-build \
            pkg-config \
            protobuf-compiler \
            rapidjson-dev \
            tzdata \
            wget && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists*

    apt-get update -y -q && \
        apt-get install -y -q \
            python3 \
            python3-pip \
            python3-dev && \
        apt-get clean && \
        rm -rf /var/lib/apt/lists/*

### 2. Install Arrow dependencies for Java
    sudo su
    apt-get install openjdk-8-jdk
    apt-get install maven
    
Verify that you have at least these versions of `java`, `javac` and `maven`:
    
    # java -version
    openjdk version "1.8.0_292"
    ..
    # javac -version
    javac 1.8.0_292
    ...
    # mvn -version
    Apache Maven 3.6.3
    ...

### 3. Install Arrow dependencies for Java Script
    sudo apt update
    sudo apt -y upgrade
    sudo apt update
    sudo apt -y install curl dirmngr apt-transport-https lsb-release ca-certificates
    curl -fsSL https://deb.nodesource.com/setup_14.x | sudo -E bash -
    sudo apt-get install -y nodejs
    sudo apt -y install yarn
    sudo apt -y install gcc g++ make

Verify that you have at least these versions of `node` and `yarn`:

    # node --version
    v14.17.2
    ...
    # yarn --version
    1.22.5
    ...
    
### 4. Install Conda
    sudo apt install curl
    curl -LO https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    sudo bash Miniconda3-latest-Linux-x86_64.sh
    
### 5. Set env vars:
    export ARROW_REPO=https://github.com/apache/arrow.git
    export BENCHMARKABLE=e6e9e6ea52b7a8f2682ffc4160168c936ca1d3e6
    export BENCHMARKABLE_TYPE=arrow-commit
    export PYTHON_VERSION=3.8
    export CONBENCH_EMAIL=...
    export CONBENCH_URL="https://conbench.ursa.dev"
    export CONBENCH_PASSWORD=...
    export MACHINE=...

### 6. Use `create_conda_env_with_arrow_python` hook to create conda env and build Arrow C++ and Arrow Python
    git clone "${ARROW_REPO}"
    pushd arrow
    git fetch -v --prune -- origin "${BENCHMARKABLE}"
    git checkout -f "${BENCHMARKABLE}"
    source dev/conbench_envs/hooks.sh create_conda_env_with_arrow_python
    popd
    
### 7. Install conbench
    git clone https://github.com/ursacomputing/conbench.git
    pushd conbench
    pip install -r requirements-cli.txt
    pip install -U PyYAML
    python setup.py install
    popd

### 8. Setup benchmarks repo
    git clone https://github.com/ursacomputing/benchmarks.git
    pushd benchmarks
    python setup.py develop
    popd
    
### 9. Setup conbench credentials
    pushd benchmarks
    touch .conbench
    echo "url: $CONBENCH_URL" >> .conbench
    echo "email: $CONBENCH_EMAIL" >> .conbench
    echo "password: $CONBENCH_PASSWORD" >> .conbench
    echo "host_name: $MACHINE" >> .conbench
    popd
 
### 10. Run Python benchmarks
    cd benchmarks
    conbench file-read ALL --iterations=3 --all=true --drop-caches=true 

### 11. Use `install_archery` hook to setup archery and run C++ benchmarks
    pushd arrow
    source dev/conbench_envs/hooks.sh install_archery
    popd
    cd benchmarks
    conbench cpp-micro --iterations=1

### 12. Use `build_arrow_r` hook to build Arrow R and run R benchmarks
    pushd arrow
    source dev/conbench_envs/hooks.sh build_arrow_r
    popd
    R -e "remotes::install_github('ursacomputing/arrowbench')"
    cd benchmarks
    conbench dataframe-to-table ALL --iterations=3 --drop-caches=true --language=R

### 13. Use `build_arrow_java` and `install_archery` hooks to build Arrow Java and run Java benchmarks
    pushd arrow
    source dev/conbench_envs/hooks.sh build_arrow_java
    source dev/conbench_envs/hooks.sh install_archery
    popd
    cd benchmarks
    conbench java-micro --iterations=1

### 14. Use `install_java_script_project_dependencies` hook to install Java Script dependencies and run Java Script benchmarks
    pushd arrow
    source dev/conbench_envs/hooks.sh install_java_script_project_dependencies
    popd
    cd benchmarks
    conbench js-micro

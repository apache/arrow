#!/bin/bash

set -x

# Setup conda environment for archery
export PATH="/home/ursa/miniforge3/bin:$PATH"
eval "$(command conda 'shell.bash' 'hook' 2> /dev/null)"
conda env update -f .buildkite/environment.yml
conda env list
conda activate archery

# Set env
export COMPOSE_DOCKER_CLI_BUILD=1
export ARCH=arm64v8
export LLVM=10
export UBUNTU=20.04

# Clone conbench cli repos
mkdir workspace
cd workspace
git clone git@github.com:ursacomputing/conbench.git
git clone git@github.com:ursacomputing/ursa-qa.git

# Update permissions of arrow and workspace dirs since docker container root user will need to be able
# to write into these dirs
cd ..
chmod -R a+rwx .

# Build docker image and container
archery docker run -v "$(pwd)"/workspace:/workspace --no-pull ubuntu-python /arrow/.buildkite/scripts/run_benchmarks.sh

#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Exit on any error
set -e

wget --quiet https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -q -p ${CONDA_PREFIX:=/opt/conda}
rm /tmp/miniconda.sh

ln -s ${CONDA_PREFIX}/etc/profile.d/conda.sh /etc/profile.d/conda.sh
echo ". ${CONDA_PREFIX}/etc/profile.d/conda.sh" >> ~/.bashrc
echo "conda activate base" >> ~/.bashrc

# Configure conda
source $MINICONDA/etc/profile.d/conda.sh
conda config --set show_channel_urls True

# Help with SSL timeouts to S3
conda config --set remote_connect_timeout_secs 12

# Setup conda-forge
conda config --add channels conda-forge

# Update packages
conda update --all -q -y

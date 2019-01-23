#!/usr/bin/env bash

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

set -e

if [ $TRAVIS_OS_NAME == "linux" ]; then
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
else
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh"
fi

function download_miniconda() {
  wget --no-verbose -O miniconda.sh $MINICONDA_URL
}

export MINICONDA=$HOME/miniconda
export CONDA_PKGS_DIRS=$HOME/.conda_packages

# If miniconda is installed already, run the activation script
if [ -d "$MINICONDA" ]; then
  source $MINICONDA/etc/profile.d/conda.sh
else
  mkdir -p $CONDA_PKGS_DIRS

  CONDA_RETRIES=5
  until [ $CONDA_RETRIES -eq 0 ]
  do
      # If wget fails to resolve the repo.continuum.io host in the DNS, it will
      # exit rather than retrying with backoff like other kinds of failures. We
      # sleep for a few seconds and retry a number of times to reduce flakiness
      download_miniconda && break
      echo "Unable to connect to repo.continuum.io, waiting and then retrying"
      CONDA_RETRIES=$[$CONDA_RETRIES - 1]
      sleep 5
  done
  if [ $CONDA_RETRIES -eq 0 ]; then
     # If we failed to download, try again so the error message will be visible
     # in Travis CI logs
     download_miniconda
  fi

  bash miniconda.sh -b -p $MINICONDA
  source $MINICONDA/etc/profile.d/conda.sh

  conda update -y -q conda
  conda config --set auto_update_conda false
  conda info -a

  conda config --set show_channel_urls True

  # Help with SSL timeouts to S3
  conda config --set remote_connect_timeout_secs 12

  conda config --add channels conda-forge
fi

conda info -a

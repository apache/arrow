#!/usr/bin/env bash

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

set -e

if [ $TRAVIS_OS_NAME == "linux" ]; then
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
else
  MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-MacOSX-x86_64.sh"
fi

wget -O miniconda.sh $MINICONDA_URL

export MINICONDA=$HOME/miniconda

bash miniconda.sh -b -p $MINICONDA
export PATH="$MINICONDA/bin:$PATH"
conda update -y -q conda
conda info -a

conda config --set show_channel_urls True
conda config --add channels https://repo.continuum.io/pkgs/free
conda config --add channels conda-forge
conda info -a

conda install --yes conda-build jinja2 anaconda-client

# faster builds, please
conda install -y nomkl

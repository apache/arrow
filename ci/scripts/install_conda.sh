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

set -e

declare -A archs
archs=([amd64]=x86_64
       [arm32v7]=armv7l
       [ppc64le]=ppc64le
       [i386]=x86)

declare -A platforms
platforms=([windows]=Windows
           [macos]=MacOSX
           [linux]=Linux)

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 <architecture> <platform> <version> <prefix>"
  exit 1
elif [[ -z ${archs[$1]} ]]; then
  echo "Unexpected architecture: ${1}"
  exit 1
elif [[ -z ${platforms[$2]} ]]; then
  echo "Unexpected platform: ${2}"
  exit 1
fi

arch=${archs[$1]}
platform=${platforms[$2]}
version=$3
prefix=$4

echo "Downloading Miniconda installer..."
wget -nv https://repo.continuum.io/miniconda/Miniconda3-${version}-${platform}-${arch}.sh -O /tmp/miniconda.sh
bash /tmp/miniconda.sh -b -p ${prefix}
rm /tmp/miniconda.sh

# Like "conda init", but for POSIX sh rather than bash
ln -s ${prefix}/etc/profile.d/conda.sh /etc/profile.d/conda.sh

# Configure
source /etc/profile.d/conda.sh
conda config --add channels conda-forge
conda config --set channel_priority strict
conda config --set show_channel_urls True
conda config --set remote_connect_timeout_secs 12

# Update and clean
conda update --all -y
conda clean --all -y

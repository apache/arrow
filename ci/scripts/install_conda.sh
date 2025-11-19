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

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <installer: miniforge3> <version> <prefix>"
  exit 1
fi

arch=$(uname -m)
platform=$(uname)
installer=$1
version=$2
prefix=$3

download_url=https://github.com/conda-forge/miniforge/releases/${version}/download/${installer^}-${platform}-${arch}.sh

echo "Downloading Miniconda installer from ${download_url} ..."

wget -nv "${download_url}" -O /tmp/installer.sh
bash /tmp/installer.sh -b -p "${prefix}"
rm /tmp/installer.sh

# Like "conda init", but for POSIX sh rather than bash
ln -s "${prefix}/etc/profile.d/conda.sh" /etc/profile.d/conda.sh

export PATH=/opt/conda/bin:$PATH

# Configure
conda config --set show_channel_urls True
conda config --set remote_connect_timeout_secs 12

# Update and clean
conda clean --all -y

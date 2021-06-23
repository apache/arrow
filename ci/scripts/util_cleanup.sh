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

# This script is Github Actions-specific to free up disk space,
# to avoid disk full errors on some builds

if [ $RUNNER_OS = "Linux" ]; then
    df -h

    # remove swap
    sudo swapoff -a
    sudo rm -f /swapfile

    # clean apt cache
    sudo apt clean

    # remove haskell, consumes 8.6 GB
    sudo rm -rf /opt/ghc

    # 1 GB
    sudo rm -rf /home/linuxbrew/.linuxbrew

    # 1+ GB
    sudo rm -rf /opt/hostedtoolcache/CodeQL

    # 1+ GB
    sudo rm -rf /usr/share/swift

    # 12 GB, but takes a lot of time to delete
    #sudo rm -rf /usr/local/lib/android

    # remove cached docker images, around 13 GB
    docker rmi $(docker image ls -aq)

    # NOTE: /usr/share/dotnet is 25 GB
fi

df -h

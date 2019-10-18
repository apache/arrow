#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# NB: this assumes that you have arrow-site cloned in the (gitignored) site directory
cd /apache-arrow/arrow/site
export GEM_HOME=$(pwd)
export BUNDLE_PATH=$(pwd)
export HOME=$(pwd)
bundle install --path .
HOST_IP=$(grep $(hostname) /etc/hosts | cut -f 1)
pushd asf-site
bundle exec jekyll serve -c ../_config.yml -s . --host ${HOST_IP} --port 4000
popd

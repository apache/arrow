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

if [ "$ARROW_CI_RUBY_AFFECTED" = "1" ]; then
    brew_log_path=brew.log
    function run_brew() {
        echo brew "$@" >> ${brew_log_path}
        brew "$@" >> ${brew_log_path} 2>&1
    }
    run_brew update
    run_brew upgrade python
    run_brew uninstall postgis
    run_brew bundle --file=$TRAVIS_BUILD_DIR/c_glib/Brewfile --verbose
    rm ${brew_log_path}
fi

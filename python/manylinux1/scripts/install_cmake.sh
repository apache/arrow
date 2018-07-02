#!/bin/bash -e
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

/opt/python/cp35-cp35m/bin/pip install cmake ninja
ln -s /opt/python/cp35-cp35m/bin/cmake /usr/bin/cmake
ln -s /opt/python/cp35-cp35m/bin/ninja /usr/bin/ninja
strip /opt/_internal/cpython-3.*/lib/python3.5/site-packages/cmake/data/bin/*

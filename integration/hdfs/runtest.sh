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

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LIBHDFS3_CONF=$HADOOP_CONF_DIR/hdfs-site.xml
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/

# execute cpp tests
pushd /build/cpp
  debug/arrow-io-hdfs-test
popd

# cannot use --pyargs with custom arguments like --hdfs or --only-hdfs, because
# pytest ignores them, see https://github.com/pytest-dev/pytest/issues/3517
export PYARROW_TEST_ONLY_HDFS=ON

pytest -v --pyargs pyarrow.tests.test_hdfs

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

source_dir=${1}/cpp
build_dir=${2}/cpp

export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath --glob)
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export LIBHDFS3_CONF=$HADOOP_CONF_DIR/hdfs-site.xml
export ARROW_LIBHDFS3_DIR=$CONDA_PREFIX/lib

libhdfs_dir=$HADOOP_HOME/lib/native
hadoop_home=$HADOOP_HOME

function use_hadoop_home() {
  unset ARROW_LIBHDFS_DIR
  export HADOOP_HOME=$hadoop_home
}

function use_libhdfs_dir() {
  unset HADOOP_HOME
  export ARROW_LIBHDFS_DIR=$libhdfs_dir
}

# execute cpp tests
export ARROW_HDFS_TEST_LIBHDFS_REQUIRE=ON
pushd ${build_dir}

debug/arrow-io-hdfs-test
debug/arrow-hdfs-test

use_libhdfs_dir
debug/arrow-io-hdfs-test
debug/arrow-hdfs-test
use_hadoop_home

popd

# cannot use --pyargs with custom arguments like --hdfs or --only-hdfs, because
# pytest ignores them, see https://github.com/pytest-dev/pytest/issues/3517
export PYARROW_TEST_HDFS=ON

export PYARROW_HDFS_TEST_LIBHDFS_REQUIRE=ON

pytest -vs --pyargs pyarrow.tests.test_fs
pytest -vs --pyargs pyarrow.tests.test_hdfs

use_libhdfs_dir
pytest -vs --pyargs pyarrow.tests.test_fs
pytest -vs --pyargs pyarrow.tests.test_hdfs
use_hadoop_home

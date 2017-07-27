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

export MINICONDA=$HOME/miniconda
export CPP_TOOLCHAIN=$HOME/cpp-toolchain

export PATH="$MINICONDA/bin:$PATH"
export CONDA_PKGS_DIRS=$HOME/.conda_packages

export ARROW_CHECKOUT=$HOME/arrow
export BUILD_DIR=$ARROW_CHECKOUT

export BUILD_OS_NAME=linux
export BUILD_TYPE=debug

export ARROW_CPP_DIR=$BUILD_DIR/cpp
export ARROW_PYTHON_DIR=$BUILD_DIR/python
export ARROW_C_GLIB_DIR=$BUILD_DIR/c_glib
export ARROW_JAVA_DIR=${BUILD_DIR}/java
export ARROW_JS_DIR=${BUILD_DIR}/js
export ARROW_INTEGRATION_DIR=$BUILD_DIR/integration

export CPP_BUILD_DIR=$BUILD_DIR/cpp-build

export ARROW_CPP_INSTALL=$BUILD_DIR/cpp-install
export ARROW_CPP_BUILD_DIR=$BUILD_DIR/cpp-build
export ARROW_C_GLIB_INSTALL=$BUILD_DIR/c-glib-install

export ARROW_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN
export PARQUET_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN

export BOOST_ROOT=$CPP_TOOLCHAIN
export PATH=$CPP_TOOLCHAIN/bin:$PATH
export LD_LIBRARY_PATH=$CPP_TOOLCHAIN/lib:$LD_LIBRARY_PATH

export VALGRIND="valgrind --tool=memcheck"

export ARROW_HOME=$CPP_TOOLCHAIN
export PARQUET_HOME=$CPP_TOOLCHAIN

# Arrow test variables

export JAVA_HOME=/usr/lib/jvm/java-7-oracle
export HADOOP_HOME=/usr/lib/hadoop
export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib/native"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$HADOOP_HOME/lib/native/

export ARROW_HDFS_TEST_HOST=arrow-hdfs
export ARROW_HDFS_TEST_PORT=9000
export ARROW_HDFS_TEST_USER=ubuntu
export ARROW_LIBHDFS_DIR=/usr/lib

export LIBHDFS3_CONF=/io/hdfs/libhdfs3-hdfs-client.xml

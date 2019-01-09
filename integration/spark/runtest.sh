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

# exit on any error
set -e

SPARK_VERSION=${SPARK_VERSION:-2.4.0}

# rsynced source directory to build java libs
arrow_src=/build/java/arrow

pushd $arrow_src/java
  ARROW_VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | sed -n -e '/^\[.*\]/ !{ /^[0-9]/ { p; q } }'`
popd

MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m -Dorg.slf4j.simpleLogger.defaultLogLevel=warn"

# build Spark with Arrow
pushd /spark/spark-${SPARK_VERSION}
  # update Spark pom with the Arrow version just installed and build Spark, need package phase for pyspark
  echo "Building Spark with Arrow $ARROW_VERSION"
  mvn -q versions:set-property -Dproperty=arrow.version -DnewVersion=$ARROW_VERSION

  build/mvn -DskipTests package -pl sql/core -pl assembly -am

  SPARK_SCALA_TESTS=(
    "org.apache.spark.sql.execution.arrow"
    "org.apache.spark.sql.execution.vectorized.ColumnarBatchSuite"
    "org.apache.spark.sql.execution.vectorized.ArrowColumnVectorSuite")

  (echo "Testing Spark:"; IFS=$'\n'; echo "${SPARK_SCALA_TESTS[*]}")

  # TODO: should be able to only build spark-sql tests with adding "-pl sql/core" but not currently working
  build/mvn -Dtest=none -DwildcardSuites=$(IFS=,; echo "${SPARK_SCALA_TESTS[*]}") test

  # Run pyarrow related Python tests only
  echo "Testing PySpark:"
  python/run-tests --modules pyspark-sql
popd

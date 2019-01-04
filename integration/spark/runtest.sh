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

export MAVEN_OPTS="-q -Xmx2g -XX:ReservedCodeCacheSize=512m"

SPARK_VERSION=${SPARK_VERSION:-2.4.0}
pushd arrow/java
  ARROW_VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | sed -n -e '/^\[.*\]/ !{ /^[0-9]/ { p; q } }'`
popd

cache_dir="/build/spark"
spark_dir="$cache_dir/spark-$SPARK_VERSION"

if [ ! -d $spark_dir ]; then
  echo "Downloading Spark source archive to $cache_dir..."
  mkdir -p $cache_dir
  wget -q -O /tmp/spark.tar.gz https://github.com/apache/spark/archive/v$SPARK_VERSION.tar.gz
  tar -xzf /tmp/spark.tar.gz -C $cache_dir
  rm /tmp/spark.tar.gz
fi

# build Spark with Arrow
pushd $spark_dir
  # update Spark pom with the Arrow version just installed and build Spark, need package phase for pyspark
  sed -i -e "s/\(.*<arrow.version>\).*\(<\/arrow.version>\)/\1$ARROW_VERSION\2/g" ./pom.xml
  echo "Building Spark with Arrow $ARROW_VERSION"
  build/mvn -DskipTests package -pl sql/core -am

  SPARK_SCALA_TESTS=(
    "org.apache.spark.sql.execution.arrow"
    "org.apache.spark.sql.execution.vectorized.ColumnarBatchSuite"
    "org.apache.spark.sql.execution.vectorized.ArrowColumnVectorSuite")

  (echo "Testing Spark:"; IFS=$'\n'; echo "${SPARK_SCALA_TESTS[*]}")

  # TODO: should be able to only build spark-sql tests with adding "-pl sql/core" but not currently working
  build/mvn -Dtest=none -DwildcardSuites=$(IFS=,; echo "${SPARK_SCALA_TESTS[*]}") test

  # Run pyarrow related Python tests only
  SPARK_PYTHON_TESTS=(
      "pyspark.sql.tests.test_arrow ArrowTests"
      "pyspark.sql.tests.test_pandas_udf PandasUDFTests"
      "pyspark.sql.tests.test_pandas_udf_scalar ScalarPandasUDFTests"
      "pyspark.sql.tests.test_pandas_udf_grouped_map GroupedMapPandasUDFTests"
      "pyspark.sql.tests.test_pandas_udf_grouped_agg GroupedAggPandasUDFTests"
      "pyspark.sql.tests.test_pandas_udf_window WindowPandasUDFTests")

  (echo "Testing PySpark:"; IFS=$'\n'; echo "${SPARK_PYTHON_TESTS[*]}")

  python/run-tests --testnames "$(IFS=,; echo "${SPARK_PYTHON_TESTS[*]}")"
popd

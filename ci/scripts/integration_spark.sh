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
set -eu

source_dir=${1}
spark_dir=${2}

# Spark branch to checkout
spark_version=${SPARK_VERSION:-master}

# Use old behavior that always dropped timezones.
export PYARROW_IGNORE_TIMEZONE=1

if [ "${SPARK_VERSION:1:2}" == "2." ]; then
  # https://github.com/apache/spark/blob/master/docs/sql-pyspark-pandas-with-arrow.md#compatibility-setting-for-pyarrow--0150-and-spark-23x-24x
  export ARROW_PRE_0_15_IPC_FORMAT=1
fi

# Get Arrow Java version
pushd ${source_dir}/java
  arrow_version=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | sed -n -e '/^\[.*\]/ !{ /^[0-9]/ { p; q } }'`
popd

export MAVEN_OPTS="-Xss256m -Xmx2g -XX:ReservedCodeCacheSize=1g -Dorg.slf4j.simpleLogger.defaultLogLevel=warn"
export MAVEN_OPTS="${MAVEN_OPTS} -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"

pushd ${spark_dir}
  echo "Building Spark ${SPARK_VERSION}"

  # Build Spark only
  build/mvn -B -DskipTests package

  # Run pyarrow related Python tests only
  spark_python_tests=(
    "pyspark.sql.tests.test_arrow")

  case "${SPARK_VERSION}" in
    v1.*|v2.*|v3.0.*|v3.1.*|v3.2.*|v3.3.*)
      old_test_modules=true
      ;;
    *)
      old_test_modules=false
      ;;
  esac
  if [ "${old_test_modules}" == "true" ]; then
    spark_python_tests+=(
      "pyspark.sql.tests.test_pandas_grouped_map"
      "pyspark.sql.tests.test_pandas_map"
      "pyspark.sql.tests.test_pandas_cogrouped_map"
      "pyspark.sql.tests.test_pandas_udf"
      "pyspark.sql.tests.test_pandas_udf_scalar"
      "pyspark.sql.tests.test_pandas_udf_grouped_agg"
      "pyspark.sql.tests.test_pandas_udf_window")
  else
    spark_python_tests+=(
      "pyspark.sql.tests.pandas.test_pandas_grouped_map"
      "pyspark.sql.tests.pandas.test_pandas_map"
      "pyspark.sql.tests.pandas.test_pandas_cogrouped_map"
      "pyspark.sql.tests.pandas.test_pandas_udf"
      "pyspark.sql.tests.pandas.test_pandas_udf_scalar"
      "pyspark.sql.tests.pandas.test_pandas_udf_grouped_agg"
      "pyspark.sql.tests.pandas.test_pandas_udf_window")
  fi

  (echo "Testing PySpark:"; IFS=$'\n'; echo "${spark_python_tests[*]}")
  python/run-tests --testnames "$(IFS=,; echo "${spark_python_tests[*]}")" --python-executables python
popd

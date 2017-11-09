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

# Set up environment and working directory
cd /apache-arrow

export ARROW_BUILD_TYPE=release
export ARROW_HOME=$(pwd)/dist
export PARQUET_HOME=$(pwd)/dist
CONDA_BASE=/home/ubuntu/miniconda
export LD_LIBRARY_PATH=$(pwd)/dist/lib:${CONDA_BASE}/lib:${LD_LIBRARY_PATH}

# Allow for --user Python installation inside Docker
export HOME=$(pwd)

# Clean up and get the Spark master branch from github
#rm -rf spark .local
#rm -rf spark
export GIT_COMMITTER_NAME="Nobody"
export GIT_COMMITTER_EMAIL="nobody@nowhere.com"
git clone https://github.com/apache/spark.git

# Install Arrow to local maven repo (in container?) and get the version
pushd arrow/java
mvn clean install -DskipTests -Drat.skip=true -Dmaven.repo.local=/apache-arrow/.m2/repository
ARROW_VERSION=`mvn org.apache.maven.plugins:maven-help-plugin:2.1.1:evaluate -Dexpression=project.version | sed -n -e '/^\[.*\]/ !{ /^[0-9]/ { p; q } }'`
popd

# Update Spark pom with the Arrow version just installed and build Spark
pushd spark
sed -i -e "s/\(.*<arrow.version>\).*\(<\/arrow.version>\)/\1$ARROW_VERSION\2/g" ./pom.xml
build/mvn clean package -DskipTests -Dmaven.repo.local=/apache-arrow/.m2/repository

# Run Arrow related Scala tests
build/mvn test -Dtest=ArrowConvertersSuite,ArrowUtilsSuite -Dmaven.repo.local=/apache-arrow/.m2/repository
popd



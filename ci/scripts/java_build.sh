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

set -ex

arrow_dir=${1}
source_dir=${1}/java
build_dir=${2}
cpp_build_dir=${build_dir}/cpp/${ARROW_BUILD_TYPE:-debug}
cdata_dist_dir=${build_dir}/java/c

: ${BUILD_DOCS_JAVA:=OFF}

if [[ "$(uname -s)" == "Linux" ]] && [[ "$(uname -m)" == "s390x" ]]; then
  # Since some files for s390_64 are not available at maven central,
  # download pre-build files from Artifactory and install them explicitly
  mvn_install="mvn install:install-file"
  wget="wget"
  artifactory_base_url="https://apache.jfrog.io/artifactory/arrow"

  artifactory_dir="protoc-binary"
  group="com.google.protobuf"
  artifact="protoc"
  ver="3.7.1"
  classifier="linux-s390_64"
  extension="exe"
  target=${artifact}-${ver}-${classifier}.${extension}
  ${wget} ${artifactory_base_url}/${artifactory_dir}/${ver}/${target}
  ${mvn_install} -DgroupId=${group} -DartifactId=${artifact} -Dversion=${ver} -Dclassifier=${classifier} -Dpackaging=${extension} -Dfile=$(pwd)/${target}
  # protoc requires libprotoc.so.18 libprotobuf.so.18
  ${wget} ${artifactory_base_url}/${artifactory_dir}/${ver}/libprotoc.so.18
  ${wget} ${artifactory_base_url}/${artifactory_dir}/${ver}/libprotobuf.so.18
  mkdir -p ${ARROW_HOME}/lib
  cp lib*.so.18 ${ARROW_HOME}/lib
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${ARROW_HOME}/lib

  artifactory_dir="protoc-gen-grpc-java-binary"
  group="io.grpc"
  artifact="protoc-gen-grpc-java"
  ver="1.30.2"
  classifier="linux-s390_64"
  extension="exe"
  target=${artifact}-${ver}-${classifier}.${extension}
  ${wget} ${artifactory_base_url}/${artifactory_dir}/${ver}/${target}
  ${mvn_install} -DgroupId=${group} -DartifactId=${artifact} -Dversion=${ver} -Dclassifier=${classifier} -Dpackaging=${extension} -Dfile=$(pwd)/${target}

  artifactory_dir="netty-binary"
  group="io.netty"
  artifact="netty-transport-native-unix-common"
  ver="4.1.48.Final"
  classifier="linux-s390_64"
  extension="jar"
  target=${artifact}-${ver}-${classifier}.${extension}
  ${wget} ${artifactory_base_url}/${artifactory_dir}/${ver}/${target}
  ${mvn_install} -DgroupId=${group} -DartifactId=${artifact} -Dversion=${ver} -Dclassifier=${classifier} -Dpackaging=${extension} -Dfile=$(pwd)/${target}
  artifact="netty-transport-native-epoll"
  extension="jar"
  target=${artifact}-${ver}-${classifier}.${extension}
  ${wget} ${artifactory_base_url}/${artifactory_dir}/${ver}/${target}
  ${mvn_install} -DgroupId=${group} -DartifactId=${artifact} -Dversion=${ver} -Dclassifier=${classifier} -Dpackaging=${extension} -Dfile=$(pwd)/${target}
fi

mvn="mvn -B -DskipTests -Drat.skip=true -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
# Use `2 * ncores` threads
mvn="${mvn} -T 2C"

pushd ${source_dir}

${mvn} install

if [ "${ARROW_JAVA_SHADE_FLATBUFFERS}" == "ON" ]; then
  ${mvn} -Pshade-flatbuffers install
fi

if [ "${ARROW_JAVA_CDATA}" = "ON" ]; then
  ${mvn} -Darrow.c.jni.dist.dir=${cdata_dist_dir} -Parrow-c-data install
fi

if [ "${ARROW_GANDIVA_JAVA}" = "ON" ]; then
  ${mvn} -Darrow.cpp.build.dir=${cpp_build_dir} -Parrow-jni install
fi

if [ "${ARROW_PLASMA}" = "ON" ]; then
  pushd ${source_dir}/plasma
  ${mvn} clean install
  popd
fi

if [ "${BUILD_DOCS_JAVA}" == "ON" ]; then
  # HTTP pooling is turned of to avoid download issues https://issues.apache.org/jira/browse/ARROW-11633
  mkdir -p ${build_dir}/docs/java/reference
  ${mvn} -Dcheckstyle.skip=true -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false install site
  rsync -a ${arrow_dir}/java/target/site/apidocs/ ${build_dir}/docs/java/reference
fi

popd

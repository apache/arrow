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
java_jni_dist_dir=${3}

: ${BUILD_DOCS_JAVA:=OFF}

if [[ "$(uname -s)" == "Linux" ]] && [[ "$(uname -m)" == "s390x" ]]; then
  # Since some files for s390_64 are not available at maven central,
  # download packages and build libraries here

  # download required packages for building libraries
  apt-get install -y -q --no-install-recommends \
      g++

  mvn_install="mvn install:install-file"
  wget="wget"
  tar="tar --no-same-owner --no-same-permissions"
  mkdir -p ${ARROW_HOME}/lib

  pushd ${ARROW_HOME}/
  pushd ${source_dir}
  protover=$(mvn help:evaluate -Dexpression=dep.protobuf-bom.version -q -DforceStdout)
  if [[ $? -ne 0 ]]; then
    echo "Error at protobuf: $protover"
    exit 1
  fi
  protover=$(echo $protover | sed "s/^[0-9]*.//")
  popd
  ${wget} https://github.com/protocolbuffers/protobuf/releases/download/v${protover}/protobuf-all-${protover}.tar.gz
  ${tar} -xf protobuf-all-${protover}.tar.gz
  pushd protobuf-${protover}
  ./configure
  make -j 2
  # protoc requires libprotoc.so.* libprotobuf.so.*
  cp ./src/.libs/libprotoc.so.* ./src/.libs/libprotobuf.so.* ${ARROW_HOME}/lib
  cp ./src/.libs/protoc ${ARROW_HOME}/lib
  popd

  group="com.google.protobuf"
  artifact="protoc"
  classifier="linux-s390_64"
  extension="exe"
  target=${artifact}
  ${mvn_install} -DgroupId=${group} -DartifactId=${artifact} -Dversion=${protover} -Dclassifier=${classifier} -Dpackaging=${extension} -Dfile=${ARROW_HOME}/lib/${target}
  cp lib*.so.* ${ARROW_HOME}/lib
  export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${ARROW_HOME}/lib


  pushd ${source_dir}
  grpcver=$(mvn help:evaluate -Dexpression=dep.grpc-bom.version -q -DforceStdout)
  if [[ $? -ne 0 ]]; then
    echo "Error at grpc: $grpcver"
    exit 1
  fi
  popd
  ${wget} https://github.com/grpc/grpc-java/archive/refs/tags/v${grpcver}.tar.gz
  ${tar} -xf v${grpcver}.tar.gz
  pushd grpc-java-${grpcver}
  echo skipAndroid=true >> gradle.properties
  CXXFLAGS="-I${ARROW_HOME}/protobuf-${protover}/src" LDFLAGS="-L${ARROW_HOME}/protobuf-${protover}/src/.libs" ./gradlew java_pluginExecutable --no-daemon
  cp ./compiler/build/exe/java_plugin/protoc-gen-grpc-java ${ARROW_HOME}/lib
  popd

  group="io.grpc"
  artifact="protoc-gen-grpc-java"
  classifier="linux-s390_64"
  extension="exe"
  target=${artifact}
  ${mvn_install} -DgroupId=${group} -DartifactId=${artifact} -Dversion=${grpcver} -Dclassifier=${classifier} -Dpackaging=${extension} -Dfile=${ARROW_HOME}/lib/${target}
  popd
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
  ${mvn} -Darrow.c.jni.dist.dir=${java_jni_dist_dir} -Parrow-c-data install
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

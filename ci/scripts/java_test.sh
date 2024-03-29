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

if [[ "${ARROW_JAVA_TEST:-ON}" != "ON" ]]; then
  exit
fi

arrow_dir=${1}
source_dir=${1}/java
java_jni_dist_dir=${3}

# For JNI
export LD_LIBRARY_PATH=${ARROW_HOME}/lib:${LD_LIBRARY_PATH}

mvn="mvn -B -Dorg.slf4j.simpleLogger.log.org.apache.maven.cli.transfer.Slf4jMavenTransferListener=warn"
# Use `2 * ncores` threads
mvn="${mvn} -T 2C"

pushd ${source_dir}

${mvn} clean test

projects=()
if [ "${ARROW_DATASET}" = "ON" ]; then
  projects+=(gandiva)
fi
if [ "${ARROW_GANDIVA}" = "ON" ]; then
  projects+=(gandiva)
fi
if [ "${ARROW_ORC}" = "ON" ]; then
  projects+=(adapter/orc)
fi
if [ "${#projects[@]}" -gt 0 ]; then
  ${mvn} clean test \
         -Parrow-jni \
         -pl $(IFS=,; echo "${projects[*]}") \
         -Darrow.cpp.build.dir=${java_jni_dist_dir}
fi

if [ "${ARROW_JAVA_CDATA}" = "ON" ]; then
  ${mvn} clean test -Parrow-c-data -pl c -Darrow.c.jni.dist.dir=${java_jni_dist_dir}
fi

echo "=== Checking third-party licenses ==="

${mvn} \
  license:add-third-party \
  -Dlicense.excludedScopes=provided,test \
  -Dlicense.excludeTransitiveDependencies=true \
  -Dlicense.failOnMissing=true \
  -Dlicense.failIfWarning=true

set +x

# Ignore grep returning 1 on no match
function safegrep { grep "$@" || test $? = 1; }

fail=0
for report in $(find . -type f -name THIRD-PARTY.txt | sort); do
  echo "=== Checking ${report} ==="
  # Include-list of safe licenses.  Allow javax.annotation since it is
  # effectively a build-only dependency (annotations have no retention). This
  # is used by gRPC and cannot yet be disabled, though this is coming
  # (https://github.com/grpc/grpc-java/issues/9179)
  bad_deps=$(cat "${report}" |
               safegrep -v -e '^$' |
               safegrep -v -E "Lists of.*dependencies" |
               safegrep -v -E "The project has no dependencies" |
               safegrep -v "javax.annotation:javax.annotation-api:1.3.2" |
               safegrep -v "The Apache Software License, Version 2.0" |
               safegrep -v "Apache License, Version 2.0" |
               safegrep -v "Apache License V2.0" |
               safegrep -v "Apache 2.0" |
               safegrep -v "Apache-2.0" |
               safegrep -v --fixed-strings "BSD 2-Clause License" |
               safegrep -v --fixed-strings "BSD-3-Clause" |
               safegrep -v "Bouncy Castle Licence" |
               safegrep -i -v "MIT license" |
               safegrep -v "Public Domain")
  if [ -n "${bad_deps}" ]; then
    echo "Found bad dependencies in ${report}:"
    echo "${bad_deps}"
    fail=$((fail + 1))
  fi
done

popd

exit ${fail}

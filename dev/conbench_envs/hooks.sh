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

set -ex

## These hooks are used by benchmark builds
# to create a conda env with Arrow dependencies and build Arrow C++, Python, etc
create_conda_env_for_benchmark_build() {
  conda create -y -q -n "${BENCHMARKABLE_TYPE}" -c conda-forge --override-channels \
    --file ci/conda_env_cpp.txt \
    --file ci/conda_env_python.txt \
    --file ci/conda_env_unix.txt \
    compilers \
    python="${PYTHON_VERSION}" \
    pandas \
    r
}

activate_conda_env_for_benchmark_build() {
  conda init bash
  conda activate "${BENCHMARKABLE_TYPE}"
}

install_arrow_python_dependencies() {
  pip install -r python/requirements-build.txt -r python/requirements-test.txt
}

set_arrow_build_and_run_env_vars() {
  set -a
  source dev/conbench_envs/benchmarks.env
  set +a
}

build_arrow_cpp() {
  export ARROW_BUILD_DIR="/tmp/arrow-cpp-$(uuidgen)"
  ci/scripts/cpp_build.sh $(pwd) $ARROW_BUILD_DIR
}

build_arrow_python() {
  mkdir -p /tmp/arrow
  ci/scripts/python_build.sh $(pwd) /tmp/arrow
}

build_arrow_r() {
  cat ci/etc/rprofile >> $(R RHOME)/etc/Rprofile.site

  # Ensure CXX20 is configured in R's Makeconf.
  # conda-forge's R may have empty CXX20 entries even though the compiler supports it.
  # Arrow requires C++20, so we need to add these settings if missing.
  MAKECONF="$(R RHOME)/etc/Makeconf"
  if [ -z "$(R CMD config CXX20)" ]; then
    echo "*** CXX20 not configured in R, adding it to Makeconf"
    cat >> "$MAKECONF" << 'EOF'

# Added for Arrow C++20 support
CXX20 = g++
CXX20FLAGS = -g -O2 $(LTO)
CXX20PICFLAGS = -fpic
CXX20STD = -std=gnu++20
SHLIB_CXX20LD = $(CXX20) $(CXX20STD)
SHLIB_CXX20LDFLAGS = -shared
EOF
  fi

  ci/scripts/r_deps.sh $(pwd) $(pwd)
  (cd r; R CMD INSTALL .;)
}

build_arrow_java() {
  mkdir -p /tmp/arrow
  ci/scripts/java_build.sh $(pwd) /tmp/arrow
}

install_archery() {
  pip install -e dev/archery
}

install_java_script_project_dependencies() {
  (cd js; yarn;)
}

create_conda_env_with_arrow_python() {
  create_conda_env_for_benchmark_build
  activate_conda_env_for_benchmark_build
  install_arrow_python_dependencies
  set_arrow_build_and_run_env_vars
  build_arrow_cpp
  build_arrow_python
}

"$@"

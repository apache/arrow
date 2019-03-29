#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eux

: ${BENCHMARK_REPETITIONS:=10}

PWD=$(cd $(dirname $BASH_SOURCE); pwd)

convert_benchmark() {
  local version=$1

  ${PWD}/convert-benchmark.py ${version}
}

run_benchmark() {
  local bin=$1
  shift

  "${bin}" \
    --benchmark_format=json \
    --benchmark_repetitions=${BENCHMARK_REPETITIONS} \
    $@
}

main() {
  local build_dir=$1
  local benchmark_bin=$2
  local benchmark_src=$3
  shift; shift; shift

  local benchmark_dir=${build_dir}/benchmarks
  mkdir -p "${benchmark_dir}"

  # Extract benchmark's version by using hash of the original source file. This
  # is not perfect, but a good enough proxy. Any change made to the benchmark
  # file can invalidate previous runs.
  local v="00000000000000000000000000000000"
  if [ -e "${benchmark_src}" ]; then
    v=$(md5sum "${benchmark_src}" | cut -d' ' -f1)
  fi

  local benchmark_name=$(basename "${benchmark_bin}")
  local orig_result=${benchmark_dir}/${benchmark_name}.json.original
  local result=${benchmark_dir}/${benchmark_name}.json

  # pass in the converter but also keep the original output for debugging
  run_benchmark "${benchmark_bin}" $@ | \
    tee ${orig_result} | \
    convert_benchmark "${v}" > "${result}"
}

main $@

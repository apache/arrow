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

add_custom_target(arrow_compute)

arrow_install_all_headers("arrow/compute")

# pkg-config support
arrow_add_pkg_config("arrow-compute")

#
# Unit tests
#

# The following kernels are always present:
#  - array_filter
#  - array_take
#  - cast
#  - dictionary_encode
#  - drop_null
#  - filter
#  - indices_nonzero
#  - take
#  - unique
#  - value_counts
#
# Tests that use additional kernels should specify REQUIRE_ALL_KERNELS to avoid
# being included in minimal builds. See: GH-34388
function(ADD_ARROW_COMPUTE_TEST REL_TEST_NAME)
  set(options REQUIRE_ALL_KERNELS)
  set(one_value_args PREFIX)
  set(multi_value_args LABELS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(ARG_REQUIRE_ALL_KERNELS AND (NOT ARROW_COMPUTE))
    return()
  endif()

  if(ARG_PREFIX)
    set(PREFIX ${ARG_PREFIX})
  else()
    set(PREFIX "arrow-compute")
  endif()

  if(ARG_LABELS)
    set(LABELS ${ARG_LABELS})
  else()
    set(LABELS "arrow_compute")
  endif()

  add_arrow_test(${REL_TEST_NAME}
                 EXTRA_LINK_LIBS
                 ${ARROW_DATASET_TEST_LINK_LIBS}
                 PREFIX
                 ${PREFIX}
                 LABELS
                 ${LABELS}
                 ${ARG_UNPARSED_ARGUMENTS})
endfunction()

add_arrow_compute_test(internals_test
                       SOURCES
                       function_test.cc
                       exec_test.cc
                       kernel_test.cc
                       light_array_test.cc
                       registry_test.cc
                       key_hash_test.cc
                       expression_test.cc)

add_arrow_benchmark(function_benchmark PREFIX "arrow-compute")

add_subdirectory(kernels)

add_subdirectory(exec)
add_subdirectory(row)

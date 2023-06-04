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

add_custom_target(arrow_dataset)

arrow_install_all_headers("arrow/dataset")

set(ARROW_DATASET_SRCS
    dataset.cc
    dataset_writer.cc
    discovery.cc
    file_base.cc
    file_ipc.cc
    partition.cc
    plan.cc
    projector.cc
    scanner.cc
    scan_node.cc)

set(ARROW_DATASET_PKG_CONFIG_REQUIRES "arrow-acero")
if(ARROW_PARQUET)
  string(APPEND ARROW_DATASET_PKG_CONFIG_REQUIRES " parquet")
endif()

set(ARROW_DATASET_STATIC_LINK_LIBS)
set(ARROW_DATASET_SHARED_LINK_LIBS)
set(ARROW_DATASET_STATIC_INSTALL_INTERFACE_LIBS)
set(ARROW_DATASET_SHARED_INSTALL_INTERFACE_LIBS)

if(ARROW_CSV)
  list(APPEND ARROW_DATASET_SRCS file_csv.cc)
endif()

if(ARROW_JSON)
  list(APPEND ARROW_DATASET_SRCS file_json.cc)
endif()

if(ARROW_ORC)
  list(APPEND ARROW_DATASET_SRCS file_orc.cc)
endif()

if(ARROW_PARQUET)
  list(APPEND ARROW_DATASET_STATIC_LINK_LIBS parquet_static)
  list(APPEND ARROW_DATASET_SHARED_LINK_LIBS parquet_shared)
  list(APPEND ARROW_DATASET_STATIC_INSTALL_INTERFACE_LIBS Parquet::parquet_static)
  list(APPEND ARROW_DATASET_SHARED_INSTALL_INTERFACE_LIBS Parquet::parquet_shared)
  list(APPEND ARROW_DATASET_SRCS file_parquet.cc)
  list(APPEND ARROW_DATASET_PRIVATE_INCLUDES ${PROJECT_SOURCE_DIR}/src/parquet)
endif()

list(APPEND ARROW_DATASET_STATIC_INSTALL_INTERFACE_LIBS ArrowAcero::arrow_acero_static)
list(APPEND ARROW_DATASET_SHARED_INSTALL_INTERFACE_LIBS ArrowAcero::arrow_acero_shared)

list(APPEND ARROW_DATASET_STATIC_LINK_LIBS arrow_acero_static ${ARROW_STATIC_LINK_LIBS})
list(APPEND ARROW_DATASET_SHARED_LINK_LIBS arrow_acero_shared)

add_arrow_lib(arrow_dataset
              CMAKE_PACKAGE_NAME
              ArrowDataset
              PKG_CONFIG_NAME
              arrow-dataset
              OUTPUTS
              ARROW_DATASET_LIBRARIES
              SOURCES
              ${ARROW_DATASET_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/dataset/pch.h>"
              DEPENDENCIES
              toolchain
              PRIVATE_INCLUDES
              ${ARROW_DATASET_PRIVATE_INCLUDES}
              SHARED_LINK_LIBS
              ${ARROW_DATASET_SHARED_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_DATASET_SHARED_INSTALL_INTERFACE_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_STATIC_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_DATASET_STATIC_INSTALL_INTERFACE_LIBS})

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(arrow_dataset_static PUBLIC ARROW_DS_STATIC)
endif()

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

foreach(LIB_TARGET ${ARROW_DATASET_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_DS_EXPORTING)
endforeach()

# Adding unit tests part of the "dataset" portion of the test suite
function(ADD_ARROW_DATASET_TEST REL_TEST_NAME)
  set(options)
  set(one_value_args PREFIX)
  set(multi_value_args LABELS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(ARG_PREFIX)
    set(PREFIX ${ARG_PREFIX})
  else()
    set(PREFIX "arrow-dataset")
  endif()

  if(ARG_LABELS)
    set(LABELS ${ARG_LABELS})
  else()
    set(LABELS "arrow_dataset")
  endif()

  add_arrow_test(${REL_TEST_NAME}
                 EXTRA_LINK_LIBS
                 ${ARROW_DATASET_TEST_LINK_LIBS}
                 PREFIX
                 ${PREFIX}
                 SOURCES
                 ${REL_TEST_NAME}.cc
                 test_util_internal.cc
                 ../acero/test_util_internal.cc
                 LABELS
                 ${LABELS}
                 ${ARG_UNPARSED_ARGUMENTS})
endfunction()

add_arrow_dataset_test(dataset_test)
add_arrow_dataset_test(dataset_writer_test)
add_arrow_dataset_test(discovery_test)
add_arrow_dataset_test(file_ipc_test)
add_arrow_dataset_test(file_test)
add_arrow_dataset_test(partition_test)
add_arrow_dataset_test(scanner_test)
add_arrow_dataset_test(subtree_test)
add_arrow_dataset_test(write_node_test)

if(ARROW_CSV)
  add_arrow_dataset_test(file_csv_test)
endif()

if(ARROW_JSON)
  add_arrow_dataset_test(file_json_test)
endif()

if(ARROW_ORC)
  add_arrow_dataset_test(file_orc_test)
endif()

if(ARROW_PARQUET)
  add_arrow_dataset_test(file_parquet_test)
endif()

if(ARROW_BUILD_BENCHMARKS)
  add_arrow_benchmark(file_benchmark PREFIX "arrow-dataset")
  add_arrow_benchmark(scanner_benchmark PREFIX "arrow-dataset")

  if(ARROW_BUILD_STATIC)
    target_link_libraries(arrow-dataset-file-benchmark PUBLIC arrow_dataset_static)
    target_link_libraries(arrow-dataset-scanner-benchmark PUBLIC arrow_dataset_static)
  else()
    target_link_libraries(arrow-dataset-file-benchmark PUBLIC arrow_dataset_shared)
    target_link_libraries(arrow-dataset-scanner-benchmark PUBLIC arrow_dataset_shared)
  endif()
endif()

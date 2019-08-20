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

# Headers: top level
arrow_install_all_headers("arrow/dataset")

# pkg-config support
arrow_add_pkg_config("arrow-dataset")

set(ARROW_DATASET_SRCS scanner.cc file_base.cc)
set(ARROW_DATASET_LINK_STATIC arrow_static)
set(ARROW_DATASET_LINK_SHARED arrow_shared)

if(ARROW_PARQUET)
  set(ARROW_DATASET_LINK_STATIC ${ARROW_DATASET_LINK_STATIC} parquet_static)
  set(ARROW_DATASET_LINK_SHARED ${ARROW_DATASET_LINK_SHARED} parquet_shared)
  set(ARROW_DATASET_SRCS ${ARROW_DATASET_SRCS} file_parquet.cc)
  set(ARROW_DATASET_PRIVATE_INCLUDES ${PROJECT_SOURCE_DIR}/src/parquet)
endif()

add_arrow_lib(arrow_dataset
              OUTPUTS
              ARROW_DATASET_LIBRARIES
              SOURCES
              ${ARROW_DATASET_SRCS}
              PRIVATE_INCLUDES
              ${ARROW_DATASET_PRIVATE_INCLUDES}
              SHARED_LINK_LIBS
              ${ARROW_DATASET_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_DATASET_LINK_STATIC})

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_DATASET_TEST_LINK_LIBS arrow_dataset_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

foreach(LIB_TARGET ${ARROW_DATASET_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_DS_EXPORTING)
endforeach()

if(NOT WIN32)
  add_arrow_test(file_test
                 EXTRA_LINK_LIBS
                 ${ARROW_DATASET_TEST_LINK_LIBS}
                 PREFIX
                 "arrow-dataset"
                 LABELS
                 "arrow_dataset")

  if(ARROW_PARQUET)
    add_arrow_test(file_parquet_test
                   EXTRA_LINK_LIBS
                   ${ARROW_DATASET_TEST_LINK_LIBS}
                   PREFIX
                   "arrow-dataset"
                   LABELS
                   "arrow_dataset")

  endif()
endif()

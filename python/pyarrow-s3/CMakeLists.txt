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

cmake_minimum_required(VERSION 3.25)

# Nothing is compiled, but Arrow's CMake package expects a language enabled.
project(pyarrow_s3 LANGUAGES CXX)

find_package(ArrowS3 REQUIRED)

# Ship only the versioned library, named the same way bundle_arrow_lib names
# it in python/CMakeLists.txt, so pyarrow's _s3fs resolves it by its SONAME
# (Linux) or install name (macOS).
get_filename_component(ARROW_S3_LIB_REAL ${ARROW_S3_SHARED_LIB} REALPATH)
get_filename_component(ARROW_S3_LIB_NAME ${ARROW_S3_SHARED_LIB} NAME_WE)
if(MSVC)
  set(ARROW_S3_LIB_INSTALLED_NAME ${ARROW_S3_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
elseif(APPLE)
  set(ARROW_S3_LIB_INSTALLED_NAME
      ${ARROW_S3_LIB_NAME}.${ARROW_SO_VERSION}${CMAKE_SHARED_LIBRARY_SUFFIX})
else()
  set(ARROW_S3_LIB_INSTALLED_NAME
      ${ARROW_S3_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${ARROW_SO_VERSION})
endif()
install(FILES ${ARROW_S3_LIB_REAL}
        DESTINATION pyarrow_s3
        RENAME ${ARROW_S3_LIB_INSTALLED_NAME})
if(MSVC)
  # The import library, previously bundled in the pyarrow wheel, for C++
  # extensions that link against Arrow S3.
  install(FILES ${ARROW_S3_IMPORT_LIB}
          DESTINATION pyarrow_s3
          RENAME ${ARROW_S3_LIB_NAME}.lib)
endif()

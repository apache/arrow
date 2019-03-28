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

cmake_minimum_required(VERSION 3.2)
set(CMAKE_CXX_STANDARD 11)

set(MLARROW_VERSION "0.14.0-SNAPSHOT")
string(REGEX MATCH
  "^[0-9]+\\.[0-9]+\\.[0-9]+" MLARROW_BASE_VERSION "${MLARROW_VERSION}")

project(mlarrow VERSION "${MLARROW_BASE_VERSION}")

# Grab CMAKE Modules from the CPP interface
set(CPP_CMAKE_MODULES "${CMAKE_SOURCE_DIR}/../cpp/cmake_modules")
if(EXISTS "${CPP_CMAKE_MODULES}")
  set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CPP_CMAKE_MODULES})
endif()

## Arrow is Required
find_package(Arrow REQUIRED)

## MATLAB is required to be installed to build MEX interfaces 
set(MATLAB_ADDITIONAL_VERSIONS "R2018a=9.4")
find_package(Matlab REQUIRED MX_LIBRARY)

# Build featherread mex file based on the arrow shared library
matlab_add_mex(NAME featherreadmex
               SRC src/featherreadmex.cc
                   src/feather_reader.cc
                   src/util/handle_status.cc
               LINK_TO ${ARROW_SHARED_LIB})
target_include_directories(featherreadmex PRIVATE ${ARROW_INCLUDE_DIR})

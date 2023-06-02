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

# ----------------------------------
# Configure libmexclass FetchContent
# ----------------------------------

set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_NAME libmexclass)
# TODO: Consider using SSH URL for the Git Repository when
# libmexclass is accessible for CI without permission issues.
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_GIT_REPOSITORY "https://github.com/mathworks/libmexclass.git")
# Use a specific Git commit hash to avoid libmexclass version changing unexpectedly.
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_GIT_TAG "44c15d0")
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_SOURCE_SUBDIR "libmexclass/cpp")

# ------------------------------------------
# Configure libmexclass Client Proxy Library
# ------------------------------------------

set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_NAME arrowproxy)
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_ROOT_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/cpp")
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/array/proxy"
                                                      "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/bit")
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_SOURCES "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/array/proxy/array.cc"
                                                  "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/bit/bit_pack_matlab_logical_array.cc"
                                                  "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/bit/bit_unpack_arrow_buffer.cc")

set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_FACTORY_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/proxy")
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_FACTORY_SOURCES "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/proxy/factory.cc")
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_INCLUDE_DIRS ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_ROOT_INCLUDE_DIR}
                                                               ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_INCLUDE_DIR}
                                                               ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_FACTORY_INCLUDE_DIR})
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_SOURCES ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_SOURCES}
                                                          ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_FACTORY_SOURCES})
# ----------------------------------------
# Configure libmexclass Client MEX Gateway
# ----------------------------------------

set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_MEX_GATEWAY_NAME gateway)
set(MATLAB_ARROW_LIBMEXCLASS_CLIENT_MEX_GATEWAY_SOURCES "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/mex/gateway.cc")

# ---------------------------------------
# Download libmexclass Using FetchContent
# ---------------------------------------

# Include libmexclass using FetchContent.
include(FetchContent)
FetchContent_Declare(
    ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_NAME}
    GIT_REPOSITORY ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_GIT_REPOSITORY}
    GIT_TAG ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_GIT_TAG}
    SOURCE_SUBDIR ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_SOURCE_SUBDIR}
)
FetchContent_MakeAvailable(
    ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_FETCH_CONTENT_NAME}
)

# ------------------------------------
# Add libmexclass Client Proxy Library
# ------------------------------------

if(NOT TARGET arrow_shared)
    message(FATAL_ERROR "The Arrow C++ libraries must be available to build the MATLAB Interface to Arrow.")
endif()

libmexclass_client_add_proxy_library(
    NAME ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_NAME}
    SOURCES ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_SOURCES}
    INCLUDE_DIRS ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_INCLUDE_DIRS}
    LINK_LIBRARIES arrow_shared
)
# Use C++17
target_compile_features(${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_NAME} PRIVATE cxx_std_17)

# When building Arrow from source, Arrow must be built before building the client Proxy library.
if(TARGET arrow_ep)
    add_dependencies(${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_NAME} arrow_ep)
endif()

# ----------------------------------
# Add libmexclass Client MEX Gateway
# ----------------------------------

libmexclass_client_add_mex_gateway(
    NAME ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_MEX_GATEWAY_NAME}
    CLIENT_PROXY_LIBRARY_NAME ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_NAME}
    SOURCES ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_MEX_GATEWAY_SOURCES}
)

# --------------------------
# Install libmexclass Client
# --------------------------

libmexclass_client_install(
    CLIENT_PROXY_LIBRARY_NAME ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_PROXY_LIBRARY_NAME}
    CLIENT_MEX_GATEWAY_NAME ${MATLAB_ARROW_LIBMEXCLASS_CLIENT_MEX_GATEWAY_NAME}
    DESTINATION ${CMAKE_INSTALL_DIR}
)

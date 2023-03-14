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

# -------
# Config
# -------

# Build configuration for libmexclass.
set(CUSTOM_PROXY_FACTORY_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/proxy;${CMAKE_SOURCE_DIR}/src/cpp")
set(CUSTOM_PROXY_FACTORY_SOURCES "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/proxy/factory.cc")
set(CUSTOM_PROXY_SOURCES "${CMAKE_SOURCE_DIR}/src/cpp/arrow/matlab/array/proxy/double_array.cc")
set(CUSTOM_PROXY_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/src/cpp;${ARROW_INCLUDE_DIR}")
set(CUSTOM_PROXY_LINK_LIBRARIES ${ARROW_LINK_LIB})
# On Windows, arrow.dll must be installed regardless of
# whether Arrow_FOUND is true or false. Therefore, we explicitly
# copy ARROW_SHARED_LIB to the installation folder +libmexclass/+proxy.
set(CUSTOM_PROXY_RUNTIME_LIBRARIES ${ARROW_SHARED_LIB})
set(CUSTOM_PROXY_FACTORY_HEADER_FILENAME "factory.h")
set(CUSTOM_PROXY_FACTORY_CLASS_NAME "arrow::matlab::proxy::Factory")

# -------
# Build
# -------

# Build libmexclass as an external project.
include(ExternalProject)
ExternalProject_Add(
    libmexclass
    # TODO: Consider using SSH URL for the Git Repository when
    # libmexclass is accessible for CI without permission issues.
    GIT_REPOSITORY https://github.com/mathworks/libmexclass.git
    GIT_TAG main
    SOURCE_SUBDIR libmexclass/cpp
    CMAKE_CACHE_ARGS "-D CUSTOM_PROXY_FACTORY_INCLUDE_DIR:STRING=${CUSTOM_PROXY_FACTORY_INCLUDE_DIR}"
                     "-D CUSTOM_PROXY_FACTORY_SOURCES:STRING=${CUSTOM_PROXY_FACTORY_SOURCES}"
                     "-D CUSTOM_PROXY_SOURCES:STRING=${CUSTOM_PROXY_SOURCES}"
                     "-D CUSTOM_PROXY_INCLUDE_DIR:STRING=${CUSTOM_PROXY_INCLUDE_DIR}"
                     "-D CUSTOM_PROXY_LINK_LIBRARIES:STRING=${CUSTOM_PROXY_LINK_LIBRARIES}"
                     "-D CUSTOM_PROXY_RUNTIME_LIBRARIES:STRING=${CUSTOM_PROXY_RUNTIME_LIBRARIES}"
                     "-D CUSTOM_PROXY_FACTORY_HEADER_FILENAME:STRING=${CUSTOM_PROXY_FACTORY_HEADER_FILENAME}"
                     "-D CUSTOM_PROXY_FACTORY_CLASS_NAME:STRING=${CUSTOM_PROXY_FACTORY_CLASS_NAME}"
    INSTALL_COMMAND ${CMAKE_COMMAND} --build . --target install
)

# When building Arrow from source, Arrow must be built before building libmexclass.
if(TARGET arrow_ep)
    add_dependencies(libmexclass arrow_ep)
endif()

add_custom_command(TARGET libmexclass
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E cmake_echo_color --green --bold --no-newline "✓ Success "
                   VERBATIM)
add_custom_command(TARGET libmexclass
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E cmake_echo_color --white "libmexclass build successful."
                   VERBATIM)

# -------
# Install
# -------

# Install libmexclass.
ExternalProject_Get_Property(libmexclass BINARY_DIR)
# Copy only the packaged folder +libmexclass from the libmexclass installation directory.
install(DIRECTORY ${BINARY_DIR}/+libmexclass DESTINATION ${CMAKE_INSTALL_DIR})

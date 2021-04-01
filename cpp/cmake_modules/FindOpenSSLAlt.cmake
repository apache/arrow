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

if(ARROW_OPENSSL_USE_SHARED)
  # Find shared OpenSSL libraries.
  set(OpenSSL_USE_STATIC_LIBS OFF)
  set(OPENSSL_USE_STATIC_LIBS OFF)
  find_package(OpenSSL)
else()
  # Find static OpenSSL headers and libs
  set(OpenSSL_USE_STATIC_LIBS ON)
  set(OPENSSL_USE_STATIC_LIBS ON)
  find_package(OpenSSL)
endif()

if(OPENSSL_FOUND)
  message(STATUS "OpenSSL found with ${OPENSSL_VERSION} version")
  if(OPENSSL_VERSION LESS "1.1.0")
    message(SEND_ERROR "The OpenSSL must be greater than or equal to 1.1.0")
  endif()
else()
  message(SEND_ERROR "Not found the OpenSSL library")
endif()

if(NOT GANDIVA_OPENSSL_LIBS)
  if(WIN32)
    if(CMAKE_VERSION VERSION_LESS 3.18)
      set(GANDIVA_OPENSSL_LIBS OpenSSL::Crypto OpenSSL::SSL)
    else()
      set(GANDIVA_OPENSSL_LIBS OpenSSL::Crypto OpenSSL::SSL OpenSSL::applink)
    endif()
  else()
    set(GANDIVA_OPENSSL_LIBS OpenSSL::Crypto OpenSSL::SSL)
  endif()
endif()

if(NOT GANDIVA_OPENSSL_INCLUDE_DIR)
  set(GANDIVA_OPENSSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR})
  message(STATUS "OpenSSL include dir: ${GANDIVA_OPENSSL_INCLUDE_DIR}")
endif()

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

if(ARROW_PROTOBUF_USE_SHARED)
  set(Protobuf_USE_STATIC_LIBS OFF)
else()
  set(Protobuf_USE_STATIC_LIBS ON)
endif()

set(find_package_args)
if(ProtobufAlt_FIND_VERSION)
  list(APPEND find_package_args ${ProtobufAlt_FIND_VERSION})
endif()
if(ProtobufAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
set(ProtobufAlt_CMAKE_PREFIX_PATH "${CMAKE_PREFIX_PATH}")
if(APPLE)
  find_program(BREW brew)
  if(BREW)
    execute_process(COMMAND ${BREW} --prefix "protobuf@21"
                    OUTPUT_VARIABLE PROTOBUF_BREW_PREFIX
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(PROTOBUF_BREW_PREFIX)
      list(PREPEND CMAKE_PREFIX_PATH ${PROTOBUF_BREW_PREFIX})
    endif()
  endif()
endif()
find_package(Protobuf ${find_package_args})
set(CMAKE_PREFIX_PATH "${ProtobufAlt_CMAKE_PREFIX_PATH}")
set(ProtobufAlt_FOUND ${Protobuf_FOUND})

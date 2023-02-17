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

find_package(Gandiva REQUIRED)

include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
                    ${JNI_INCLUDE_DIRS} ${JNI_HEADERS_DIR})

add_jar(arrow_java_jni_gandiva_jar
        src/main/java/org/apache/arrow/gandiva/evaluator/ConfigurationBuilder.java
        src/main/java/org/apache/arrow/gandiva/evaluator/JniWrapper.java
        src/main/java/org/apache/arrow/gandiva/evaluator/ExpressionRegistryJniHelper.java
        src/main/java/org/apache/arrow/gandiva/exceptions/GandivaException.java
        GENERATE_NATIVE_HEADERS
        arrow_java_jni_gandiva_headers)

set(GANDIVA_PROTO_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR})
set(GANDIVA_PROTO_OUTPUT_FILES "${GANDIVA_PROTO_OUTPUT_DIR}/Types.pb.cc"
                               "${GANDIVA_PROTO_OUTPUT_DIR}/Types.pb.h")

set_source_files_properties(${GANDIVA_PROTO_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

set(GANDIVA_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
get_filename_component(GANDIVA_PROTO_FILE_ABSOLUTE ${GANDIVA_PROTO_DIR}/Types.proto
                       ABSOLUTE)

find_package(Protobuf REQUIRED)
add_custom_command(OUTPUT ${GANDIVA_PROTO_OUTPUT_FILES}
                   COMMAND protobuf::protoc --proto_path ${GANDIVA_PROTO_DIR} --cpp_out
                           ${GANDIVA_PROTO_OUTPUT_DIR} ${GANDIVA_PROTO_FILE_ABSOLUTE}
                   DEPENDS ${GANDIVA_PROTO_FILE_ABSOLUTE}
                   COMMENT "Running Protobuf compiler on Types.proto"
                   VERBATIM)

add_custom_target(garrow_java_jni_gandiva_proto ALL DEPENDS ${GANDIVA_PROTO_OUTPUT_FILES})
add_library(arrow_java_jni_gandiva SHARED
            src/main/cpp/config_builder.cc
            src/main/cpp/config_holder.cc
            src/main/cpp/expression_registry_helper.cc
            src/main/cpp/jni_common.cc
            ${GANDIVA_PROTO_OUTPUT_FILES})
set_property(TARGET arrow_java_jni_gandiva PROPERTY OUTPUT_NAME "gandiva_jni")
target_link_libraries(arrow_java_jni_gandiva
                      arrow_java_jni_gandiva_headers
                      jni
                      protobuf::libprotobuf
                      Gandiva::gandiva_static)

# Localize thirdparty symbols using a linker version script. This hides them
# from the client application. The OS X linker does not support the
# version-script option.
if(CMAKE_VERSION VERSION_LESS 3.18)
  if(APPLE OR WIN32)
    set(CXX_LINKER_SUPPORTS_VERSION_SCRIPT FALSE)
  else()
    set(CXX_LINKER_SUPPORTS_VERSION_SCRIPT TRUE)
  endif()
else()
  include(CheckLinkerFlag)
  check_linker_flag(CXX
                    "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/main/cpp/symbols.map"
                    CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
endif()
# filter out everything that is not needed for the jni bridge
# statically linked stdc++ has conflicts with stdc++ loaded by other libraries.
if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
  set_target_properties(arrow_java_jni_gandiva
                        PROPERTIES LINK_FLAGS
                                   "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/main/cpp/symbols.map"
  )
endif()

install(TARGETS arrow_java_jni_gandiva
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

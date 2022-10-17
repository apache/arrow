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

find_package(Plasma REQUIRED)

include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
                    ${JNI_INCLUDE_DIRS} ${JNI_HEADERS_DIR})

add_jar(arrow_java_jni_plasma_jar
        src/main/java/org/apache/arrow/plasma/PlasmaClientJNI.java
        src/main/java/org/apache/arrow/plasma/exceptions/DuplicateObjectException.java
        src/main/java/org/apache/arrow/plasma/exceptions/PlasmaClientException.java
        src/main/java/org/apache/arrow/plasma/exceptions/PlasmaOutOfMemoryException.java
        GENERATE_NATIVE_HEADERS
        arrow_java_jni_plasma_headers)

add_library(arrow_java_jni_plasma SHARED src/main/cpp/plasma_client.cc)
set_property(TARGET arrow_java_jni_plasma PROPERTY OUTPUT_NAME "plasma_java")
target_link_libraries(arrow_java_jni_plasma arrow_java_jni_plasma_headers jni
                      Plasma::plasma_static)

if(APPLE)
  set_target_properties(arrow_java_jni_plasma PROPERTIES LINK_FLAGS
                                                         "-undefined dynamic_lookup")
endif()

install(TARGETS arrow_java_jni_plasma
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

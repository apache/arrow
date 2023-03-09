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

find_package(ArrowDataset REQUIRED)
find_package(ArrowSubstrait REQUIRED)

include_directories(${CMAKE_CURRENT_BINARY_DIR} ${CMAKE_CURRENT_SOURCE_DIR}
                    ${JNI_INCLUDE_DIRS} ${JNI_HEADERS_DIR})

add_jar(arrow_java_jni_dataset_jar
        src/main/java/org/apache/arrow/dataset/jni/JniLoader.java
        src/main/java/org/apache/arrow/dataset/jni/JniWrapper.java
        src/main/java/org/apache/arrow/dataset/file/JniWrapper.java
        src/main/java/org/apache/arrow/dataset/jni/NativeMemoryPool.java
        src/main/java/org/apache/arrow/dataset/jni/ReservationListener.java
        src/main/java/org/apache/arrow/dataset/substrait/JniWrapper.java
        GENERATE_NATIVE_HEADERS
        arrow_java_jni_dataset_headers)

add_library(arrow_java_jni_dataset SHARED src/main/cpp/jni_wrapper.cc
                                          src/main/cpp/jni_util.cc)
set_property(TARGET arrow_java_jni_dataset PROPERTY OUTPUT_NAME "arrow_dataset_jni")
target_link_libraries(arrow_java_jni_dataset
                      arrow_java_jni_dataset_headers
                      jni
                      ArrowDataset::arrow_dataset_static
                      ArrowSubstrait::arrow_substrait_static)

if(BUILD_TESTING)
  add_executable(arrow-java-jni-dataset-test src/main/cpp/jni_util_test.cc
                                             src/main/cpp/jni_util.cc)
  target_link_libraries(arrow-java-jni-dataset-test arrow_java_test)
  add_test(NAME arrow-java-jni-dataset-test COMMAND arrow-java-jni-dataset-test)
endif()

install(TARGETS arrow_java_jni_dataset
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

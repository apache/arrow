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

project(gandiva_java)

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

message("generating headers to ${JNI_HEADERS_DIR}/jni")

# generate_native_headers is available only from java8
# centos5 does not have java8 images, so supporting java 7 too.
# unfortunately create_javah does not work in java8 correctly.
if(ARROW_GANDIVA_JAVA7)
  add_jar(
    gandiva_java
    src/main/java/org/apache/arrow/gandiva/evaluator/ConfigurationBuilder.java
    src/main/java/org/apache/arrow/gandiva/evaluator/JniWrapper.java
    src/main/java/org/apache/arrow/gandiva/evaluator/ExpressionRegistryJniHelper.java
    src/main/java/org/apache/arrow/gandiva/exceptions/GandivaException.java)

  create_javah(TARGET
               gandiva_jni_headers
               CLASSES
               org.apache.arrow.gandiva.evaluator.ConfigurationBuilder
               org.apache.arrow.gandiva.evaluator.JniWrapper
               org.apache.arrow.gandiva.evaluator.ExpressionRegistryJniHelper
               org.apache.arrow.gandiva.exceptions.GandivaException
               DEPENDS
               gandiva_java
               CLASSPATH
               gandiva_java
               OUTPUT_DIR
               ${JNI_HEADERS_DIR}/jni)
else()
  add_jar(
    gandiva_java
    src/main/java/org/apache/arrow/gandiva/evaluator/ConfigurationBuilder.java
    src/main/java/org/apache/arrow/gandiva/evaluator/JniWrapper.java
    src/main/java/org/apache/arrow/gandiva/evaluator/ExpressionRegistryJniHelper.java
    src/main/java/org/apache/arrow/gandiva/exceptions/GandivaException.java
    GENERATE_NATIVE_HEADERS
    gandiva_jni_headers
    DESTINATION
    ${JNI_HEADERS_DIR}/jni)
endif()

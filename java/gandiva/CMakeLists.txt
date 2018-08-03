# Copyright (C) 2017-2018 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

project(gandiva_java)

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

message("generating headers to ${JNI_HEADERS_DIR}/jni")
add_jar(
  gandiva_java
    src/main/java/org/apache/arrow/gandiva/evaluator/ConfigurationBuilder.java
    src/main/java/org/apache/arrow/gandiva/evaluator/JniWrapper.java
    src/main/java/org/apache/arrow/gandiva/evaluator/ExpressionRegistryJniHelper.java
    src/main/java/org/apache/arrow/gandiva/exceptions/GandivaException.java
  GENERATE_NATIVE_HEADERS gandivajni_headers
    DESTINATION ${JNI_HEADERS_DIR}/jni
)

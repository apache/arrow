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

#
# arrow_orc_java
#

# Headers: top level

project(arrow_orc_java)

# Find java/jni
include(FindJava)
include(UseJava)
include(FindJNI)

message("generating headers to ${JNI_HEADERS_DIR}")

add_jar(
        arrow_orc_java
        src/main/java/org/apache/arrow/adapter/orc/OrcReaderJniWrapper.java
        src/main/java/org/apache/arrow/adapter/orc/OrcStripeReaderJniWrapper.java
        src/main/java/org/apache/arrow/adapter/orc/OrcMemoryJniWrapper.java
        src/main/java/org/apache/arrow/adapter/orc/OrcJniUtils.java
        src/main/java/org/apache/arrow/adapter/orc/OrcRecordBatch.java
        src/main/java/org/apache/arrow/adapter/orc/OrcFieldNode.java
        GENERATE_NATIVE_HEADERS arrow_orc_java-native
        DESTINATION ${JNI_HEADERS_DIR}
)

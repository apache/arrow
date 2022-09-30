/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module arrow.vector {
    exports org.apache.arrow.vector;
    exports org.apache.arrow.vector.compression;
    exports org.apache.arrow.vector.dictionary;
    exports org.apache.arrow.vector.ipc;
    exports org.apache.arrow.vector.ipc.message;
    exports org.apache.arrow.vector.types;
    exports org.apache.arrow.vector.types.pojo;
    exports org.apache.arrow.vector.validate;
    requires arrow.memory.core;
    requires org.apache.arrow.flatbuf;
    requires com.fasterxml.jackson.databind;
    requires com.fasterxml.jackson.annotation;
    requires io.netty.common;
    requires java.sql;
    requires com.fasterxml.jackson.datatype.jsr310;
    requires org.apache.commons.codec;
    requires flatbuffers.java;
//    requires org.slf4j;
}
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

module org.apache.arrow.vector {
  exports org.apache.arrow.vector;
  exports org.apache.arrow.vector.compare;
  exports org.apache.arrow.vector.compare.util;
  exports org.apache.arrow.vector.complex;
  exports org.apache.arrow.vector.complex.impl;
  exports org.apache.arrow.vector.complex.reader;
  exports org.apache.arrow.vector.complex.writer;
  exports org.apache.arrow.vector.compression;
  exports org.apache.arrow.vector.dictionary;
  exports org.apache.arrow.vector.holders;
  exports org.apache.arrow.vector.ipc;
  exports org.apache.arrow.vector.ipc.message;
  exports org.apache.arrow.vector.table;
  exports org.apache.arrow.vector.types;
  exports org.apache.arrow.vector.types.pojo;
  exports org.apache.arrow.vector.util;
  exports org.apache.arrow.vector.validate;

  opens org.apache.arrow.vector.types.pojo to com.fasterxml.jackson.databind;

  requires com.fasterxml.jackson.annotation;
  requires com.fasterxml.jackson.core;
  requires com.fasterxml.jackson.databind;
  requires com.fasterxml.jackson.datatype.jsr310;
  requires flatbuffers.java;
  requires jdk.unsupported;
  requires org.apache.arrow.format;
  requires org.apache.arrow.memory.core;
  requires org.apache.commons.codec;
  requires org.eclipse.collections.impl;
  requires org.slf4j;
}

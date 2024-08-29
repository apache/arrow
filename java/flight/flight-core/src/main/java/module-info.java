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

module org.apache.arrow.flight.core {
  exports org.apache.arrow.flight;
  exports org.apache.arrow.flight.auth;
  exports org.apache.arrow.flight.auth2;
  exports org.apache.arrow.flight.client;
  exports org.apache.arrow.flight.impl;
  exports org.apache.arrow.flight.sql.impl;

  requires com.fasterxml.jackson.databind;
  requires com.google.common;
  requires com.google.errorprone.annotations;
  requires io.grpc;
  requires io.grpc.internal;
  requires io.grpc.netty;
  requires io.grpc.protobuf;
  requires io.grpc.stub;
  requires io.netty.common;
  requires io.netty.handler;
  requires io.netty.transport;
  requires org.apache.arrow.format;
  requires org.apache.arrow.memory.core;
  requires org.apache.arrow.vector;
  requires protobuf.java;
  requires org.slf4j;
}

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

package org.apache.arrow.flight.grpc;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.flight.CallHeaders;

import io.grpc.Metadata;
import io.grpc.Metadata.Key;

/**
 * A mutable adapter between the gRPC Metadata object and the Flight headers interface.
 *
 * <p>This allows us to present the headers (metadata) from gRPC without copying to/from our own object.
 */
public class MetadataAdapter implements CallHeaders {

  private final Metadata metadata;

  public MetadataAdapter(Metadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public String get(String key) {
    return this.metadata.get(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
  }

  @Override
  public byte[] getByte(String key) {
    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return this.metadata.get(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER));
    }
    return get(key).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Iterable<String> getAll(String key) {
    return this.metadata.getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));
  }

  @Override
  public Iterable<byte[]> getAllByte(String key) {
    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return this.metadata.getAll(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER));
    }
    return StreamSupport.stream(getAll(key).spliterator(), false)
        .map(String::getBytes).collect(Collectors.toList());
  }

  @Override
  public void insert(String key, String value) {
    this.metadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
  }

  @Override
  public void insert(String key, byte[] value) {
    this.metadata.put(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER), value);
  }

  @Override
  public Set<String> keys() {
    return new HashSet<>(this.metadata.keys());
  }

  @Override
  public boolean containsKey(String key) {
    if (key.endsWith("-bin")) {
      final Metadata.Key<?> grpcKey = Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER);
      return this.metadata.containsKey(grpcKey);
    }
    final Metadata.Key<?> grpcKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
    return this.metadata.containsKey(grpcKey);
  }

  @Override
  public String toString() {
    return this.metadata.toString();
  }
}

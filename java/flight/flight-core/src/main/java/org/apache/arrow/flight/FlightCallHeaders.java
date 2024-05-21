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

package org.apache.arrow.flight;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import io.grpc.Metadata;

/**
 * An implementation of the Flight headers interface for headers.
 */
public class FlightCallHeaders implements CallHeaders {
  private final Multimap<String, Object> keysAndValues;

  public FlightCallHeaders() {
    this.keysAndValues = ArrayListMultimap.create();
  }

  @Override
  public String get(String key) {
    final Collection<Object> values = this.keysAndValues.get(key);
    if (values.isEmpty()) {
      return null;
    }

    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return new String((byte[]) Iterables.get(values, 0), StandardCharsets.UTF_8);
    }

    return (String) Iterables.get(values, 0);
  }

  @Override
  public byte[] getByte(String key) {
    final Collection<Object> values = this.keysAndValues.get(key);
    if (values.isEmpty()) {
      return null;
    }

    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return (byte[]) Iterables.get(values, 0);
    }

    return ((String) Iterables.get(values, 0)).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public Iterable<String> getAll(String key) {
    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return this.keysAndValues.get(key).stream().map(o -> new String((byte[]) o, StandardCharsets.UTF_8))
          .collect(Collectors.toList());
    }
    return (Collection<String>) (Collection<?>) this.keysAndValues.get(key);
  }

  @Override
  public Iterable<byte[]> getAllByte(String key) {
    if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
      return (Collection<byte[]>) (Collection<?>) this.keysAndValues.get(key);
    }
    return this.keysAndValues.get(key).stream().map(o -> ((String) o).getBytes(StandardCharsets.UTF_8))
        .collect(Collectors.toList());
  }

  @Override
  public void insert(String key, String value) {
    this.keysAndValues.put(key, value);
  }

  @Override
  public void insert(String key, byte[] value) {
    Preconditions.checkArgument(key.endsWith("-bin"), "Binary header is named %s. It must end with %s", key, "-bin");
    Preconditions.checkArgument(key.length() > "-bin".length(), "empty key name");

    this.keysAndValues.put(key, value);
  }

  @Override
  public Set<String> keys() {
    return this.keysAndValues.keySet();
  }

  @Override
  public boolean containsKey(String key) {
    return this.keysAndValues.containsKey(key);
  }

  @Override
  public String toString() {
    return this.keysAndValues.toString();
  }
}

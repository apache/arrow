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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * metadata container specific to the binary metadata held in the grpc trailer.
 */
public class ErrorFlightMetadata implements CallHeaders {
  private final Multimap<String, byte[]> metadata = LinkedListMultimap.create();

  public ErrorFlightMetadata() {
  }


  @Override
  public String get(String key) {
    return new String(getByte(key), StandardCharsets.US_ASCII);
  }

  @Override
  public byte[] getByte(String key) {
    return Iterables.getLast(metadata.get(key));
  }

  @Override
  public Iterable<String> getAll(String key) {
    return StreamSupport.stream(
        getAllByte(key).spliterator(), false)
        .map(b -> new String(b, StandardCharsets.US_ASCII))
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<byte[]> getAllByte(String key) {
    return metadata.get(key);
  }

  @Override
  public void insert(String key, String value) {
    metadata.put(key, value.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void insert(String key, byte[] value) {
    metadata.put(key, value);
  }

  @Override
  public Set<String> keys() {
    return metadata.keySet();
  }

  @Override
  public boolean containsKey(String key) {
    return metadata.containsKey(key);
  }
}

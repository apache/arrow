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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Maps;

/**
 * Holder class for metadata associated with errors exposed via underlying transport.
 */
public class FlightMetadata {
  private final Map<String, byte[]> metadata = Maps.newHashMap();

  public FlightMetadata() {
  }

  public byte[] get(String key) {
    return metadata.get(key);
  }

  public void put(String key, byte[] value) {
    metadata.put(key, value);
  }

  public Set<String> keys() {
    return metadata.keySet();
  }

  public boolean containsKey(String key) {
    return metadata.containsKey(key);
  }

  public <T> T get(String key, Function<byte[], T> transform) {
    return transform.apply(metadata.get(key));
  }

  public int size() {
    return metadata.size();
  }
}

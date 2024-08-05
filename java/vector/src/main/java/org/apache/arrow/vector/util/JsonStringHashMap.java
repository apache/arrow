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
package org.apache.arrow.vector.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;

/**
 * Simple class that extends the regular java.util.HashMap but overrides the toString() method of
 * the HashMap class to produce a JSON string instead
 *
 * @param <K> The type of the key for the map.
 * @param <V> The type of the value for the map.
 */
public class JsonStringHashMap<K, V> extends LinkedHashMap<K, V> {

  private static final ObjectMapper MAPPER = ObjectMapperFactory.newObjectMapper();

  @Override
  public final String toString() {
    try {
      return MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize hash map to JSON string", e);
    }
  }
}

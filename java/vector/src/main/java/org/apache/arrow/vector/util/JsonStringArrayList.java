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

import java.util.ArrayList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Extension of {@link ArrayList} that {@link #toString()} method returns the serialized JSON
 * version of its members (or throws an exception if they can't be converted to JSON).
 *
 * @param <E> Type of value held in the list.
 */
public class JsonStringArrayList<E> extends ArrayList<E> {

  private static ObjectMapper mapper;

  static {
    mapper = new ObjectMapper();
  }

  public JsonStringArrayList() {
    super();
  }

  public JsonStringArrayList(int size) {
    super(size);
  }

  @Override
  public final String toString() {
    try {
      return mapper.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize array list to JSON string", e);
    }
  }
}

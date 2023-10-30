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

package org.apache.arrow.vector.dictionary;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;

/**
 * A manager for association of dictionary IDs to their corresponding {@link Dictionary}.
 */
public interface DictionaryProvider {

  /** Return the dictionary for the given ID. */
  Dictionary lookup(long id);

  /** Get all dictionary IDs. */
  Set<Long> getDictionaryIds();

  /**
   * Implementation of {@link DictionaryProvider} that is backed by a hash-map.
   */
  class MapDictionaryProvider implements AutoCloseable, DictionaryProvider {

    private final Map<Long, Dictionary> map;

    /**
     * Constructs a new instance from the given dictionaries.
     */
    public MapDictionaryProvider(Dictionary... dictionaries) {
      this.map = new HashMap<>();
      for (Dictionary dictionary : dictionaries) {
        put(dictionary);
      }
    }

    /**
     * Initialize the map structure from another provider, but with empty vectors.
     *
     * @param other the {@link DictionaryProvider} to copy the ids and fields from
     * @param allocator allocator to create the empty vectors
     */
    // This is currently called using JPype by the integration tests.
    @VisibleForTesting
    public void copyStructureFrom(DictionaryProvider other, BufferAllocator allocator) {
      for (Long id : other.getDictionaryIds()) {
        Dictionary otherDict = other.lookup(id);
        Dictionary newDict = new Dictionary(otherDict.getVector().getField().createVector(allocator),
                otherDict.getEncoding());
        put(newDict);
      }
    }

    public void put(Dictionary dictionary) {
      map.put(dictionary.getEncoding().getId(), dictionary);
    }

    @Override
    public final Set<Long> getDictionaryIds() {
      return map.keySet();
    }

    @Override
    public Dictionary lookup(long id) {
      return map.get(id);
    }

    @Override
    public void close() {
      for (Dictionary dictionary : map.values()) {
        dictionary.getVector().close();
      }
    }
  }
}

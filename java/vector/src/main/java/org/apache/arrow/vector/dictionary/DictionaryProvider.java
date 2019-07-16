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

/**
 * A manager for association of dictionary IDs to their corresponding {@link Dictionary}.
 */
public interface DictionaryProvider {

  /** Return the dictionary for the given ID. */
  Dictionary lookup(long id);

  /**
   * Implementation of {@link DictionaryProvider} that is backed by a hash-map.
   */
  class MapDictionaryProvider implements DictionaryProvider {

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

    public void put(Dictionary dictionary) {
      map.put(dictionary.getEncoding().getId(), dictionary);
    }

    public final Set<Long> getDictionaryIds() {
      return map.keySet();
    }

    @Override
    public Dictionary lookup(long id) {
      return map.get(id);
    }
  }
}

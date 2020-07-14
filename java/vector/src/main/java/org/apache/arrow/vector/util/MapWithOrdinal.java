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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of a map that supports constant time look-up by a generic key or an ordinal.
 *
 * <p>This class extends the functionality a regular {@link Map} with ordinal lookup support.
 * Upon insertion an unused ordinal is assigned to the inserted (key, value) tuple.
 * Upon update the same ordinal id is re-used while value is replaced.
 * Upon deletion of an existing item, its corresponding ordinal is recycled and could be used by another item.
 *
 * <p>For any instance with N items, this implementation guarantees that ordinals are in the range of [0, N). However,
 * the ordinal assignment is dynamic and may change after an insertion or deletion. Consumers of this class are
 * responsible for explicitly checking the ordinal corresponding to a key via
 * {@link MultiMapWithOrdinal#getOrdinal(Object)} before attempting to execute a lookup
 * with an ordinal.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface MapWithOrdinal<K, V> {
  V getByOrdinal(int id);

  int getOrdinal(K key);

  int size();

  boolean isEmpty();

  V get(K key);

  Collection<V> getAll(K key);

  boolean put(K key, V value, boolean overwrite);

  Collection<V> values();

  boolean containsKey(K key);

  boolean remove(K key, V value);

  boolean removeAll(K key);

  void clear();

  Set<K> keys();
}

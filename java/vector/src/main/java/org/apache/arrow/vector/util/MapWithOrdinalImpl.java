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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An implementation of map that supports constant time look-up by a generic key or an ordinal.
 *
 * <p>This class extends the functionality a regular {@link Map} with ordinal lookup support. Upon
 * insertion an unused ordinal is assigned to the inserted (key, value) tuple. Upon update the same
 * ordinal id is re-used while value is replaced. Upon deletion of an existing item, its
 * corresponding ordinal is recycled and could be used by another item.
 *
 * <p>For any instance with N items, this implementation guarantees that ordinals are in the range
 * of [0, N). However, the ordinal assignment is dynamic and may change after an insertion or
 * deletion. Consumers of this class are responsible for explicitly checking the ordinal
 * corresponding to a key via {@link MapWithOrdinalImpl#getOrdinal(Object)} before attempting to
 * execute a lookup with an ordinal.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class MapWithOrdinalImpl<K, V> implements MapWithOrdinal<K, V> {

  private final Map<K, Map.Entry<Integer, V>> primary = new LinkedHashMap<>();
  private final IntObjectHashMap<V> secondary = new IntObjectHashMap<>();

  private final Map<K, V> delegate =
      new Map<K, V>() {
        @Override
        public boolean isEmpty() {
          return size() == 0;
        }

        @Override
        public int size() {
          return primary.size();
        }

        @Override
        public boolean containsKey(Object key) {
          return primary.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
          return primary.containsValue(value);
        }

        @Override
        public V get(Object key) {
          Entry<Integer, V> pair = primary.get(key);
          if (pair != null) {
            return pair.getValue();
          }
          return null;
        }

        @Override
        public V put(K key, V value) {
          final Entry<Integer, V> oldPair = primary.get(key);
          // if key exists try replacing otherwise, assign a new ordinal identifier
          final int ordinal = oldPair == null ? primary.size() : oldPair.getKey();
          primary.put(key, new AbstractMap.SimpleImmutableEntry<>(ordinal, value));
          secondary.put(ordinal, value);
          return oldPair == null ? null : oldPair.getValue();
        }

        @Override
        public V remove(Object key) {
          final Entry<Integer, V> oldPair = primary.remove(key);
          if (oldPair != null) {
            final int lastOrdinal = secondary.size();
            final V last = secondary.get(lastOrdinal);
            // normalize mappings so that all numbers until primary.size() is assigned
            // swap the last element with the deleted one
            secondary.put(oldPair.getKey(), last);
            primary.put((K) key, new AbstractMap.SimpleImmutableEntry<>(oldPair.getKey(), last));
          }
          return oldPair == null ? null : oldPair.getValue();
        }

        @Override
        public void putAll(Map<? extends K, ? extends V> m) {
          throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
          primary.clear();
          secondary.clear();
        }

        @Override
        public Set<K> keySet() {
          return primary.keySet();
        }

        @Override
        public Collection<V> values() {
          return secondary.values();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
          return primary.entrySet().stream()
              .map(
                  entry ->
                      new AbstractMap.SimpleImmutableEntry<>(
                          entry.getKey(), entry.getValue().getValue()))
              .collect(Collectors.toSet());
        }
      };

  /**
   * Returns the value corresponding to the given ordinal.
   *
   * @param id ordinal value for lookup
   * @return an instance of V
   */
  @Override
  public V getByOrdinal(int id) {
    return secondary.get(id);
  }

  /**
   * Returns the ordinal corresponding to the given key.
   *
   * @param key key for ordinal lookup
   * @return ordinal value corresponding to key if it exists or -1
   */
  @Override
  public int getOrdinal(K key) {
    Map.Entry<Integer, V> pair = primary.get(key);
    if (pair != null) {
      return pair.getKey();
    }
    return -1;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public Collection<V> getAll(K key) {
    if (delegate.containsKey(key)) {
      List<V> list = new ArrayList<>(1);
      list.add(get(key));
      return list;
    }
    return null;
  }

  @Override
  public V get(K key) {
    return delegate.get(key);
  }

  /**
   * Inserts the tuple (key, value) into the map extending the semantics of {@link Map#put} with
   * automatic ordinal assignment. A new ordinal is assigned if key does not exists. Otherwise the
   * same ordinal is re-used but the value is replaced.
   *
   * @see java.util.Map#put
   */
  @Override
  public boolean put(K key, V value, boolean overwrite) {
    return delegate.put(key, value) != null;
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public boolean remove(K key, V value) {
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  /**
   * Removes the element corresponding to the key if exists extending the semantics of {@link
   * java.util.Map#remove} with ordinal re-cycling. The ordinal corresponding to the given key may
   * be re-assigned to another tuple. It is important that consumer checks the ordinal value via
   * {@link MapWithOrdinalImpl#getOrdinal(Object)} before attempting to look-up by ordinal.
   *
   * @see java.util.Map#remove
   */
  @Override
  public boolean removeAll(K key) {
    return delegate.remove(key) != null;
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public Set<K> keys() {
    return delegate.keySet();
  }
}

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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.netty.util.collection.IntObjectHashMap;

/**
 * An implementation of a multimap that supports constant time look-up by a generic key or an ordinal.
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
public class MultiMapWithOrdinal<K, V> implements MapWithOrdinal<K, V> {

  private final Map<K, Set<Integer>> keyToOrdinal = new LinkedHashMap<>();
  private final IntObjectHashMap<V> ordinalToValue = new IntObjectHashMap<>();

  /**
   * Returns the value corresponding to the given ordinal.
   *
   * @param id ordinal value for lookup
   * @return an instance of V
   */
  @Override
  public V getByOrdinal(int id) {
    return ordinalToValue.get(id);
  }

  /**
   * Returns the ordinal corresponding to the given key.
   *
   * @param key key for ordinal lookup
   * @return ordinal value corresponding to key if it exists or -1
   */
  @Override
  public int getOrdinal(K key) {
    Set<Integer> pair = getOrdinals(key);
    if (!pair.isEmpty()) {
      return pair.iterator().next();
    }
    return -1;
  }

  private Set<Integer> getOrdinals(K key) {
    return keyToOrdinal.getOrDefault(key, new HashSet<>());
  }

  @Override
  public int size() {
    return ordinalToValue.size();
  }

  @Override
  public boolean isEmpty() {
    return ordinalToValue.isEmpty();
  }

  /**
   * get set of values for key.
   */
  @Override
  public V get(K key) {
    Set<Integer> ordinals = keyToOrdinal.get(key);
    if (ordinals == null) {
      return null;
    }
    return ordinals.stream().map(ordinalToValue::get).collect(Collectors.toList()).get(0);
  }

  /**
   * get set of values for key.
   */
  @Override
  public Collection<V> getAll(K key) {
    Set<Integer> ordinals = keyToOrdinal.get(key);
    if (ordinals == null) {
      return null;
    }
    return ordinals.stream().map(ordinalToValue::get).collect(Collectors.toList());
  }

  /**
   * Inserts the tuple (key, value) into the multimap with automatic ordinal assignment.
   *
   * A new ordinal is assigned if key/value pair does not exists.
   *
   * If overwrite is true the existing key will be overwritten with value else value will be appended to the multimap.
   */
  @Override
  public boolean put(K key, V value, boolean overwrite) {
    if (overwrite) {
      removeAll(key);
    }
    Set<Integer> ordinalSet = getOrdinals(key);
    int nextOrdinal = ordinalToValue.size();
    ordinalToValue.put(nextOrdinal, value);
    boolean changed = ordinalSet.add(nextOrdinal);
    keyToOrdinal.put(key, ordinalSet);
    return changed;
  }

  @Override
  public Collection<V> values() {
    return ordinalToValue.values();
  }

  @Override
  public boolean containsKey(K key) {
    return keyToOrdinal.containsKey(key);
  }

  /**
   * Removes the element corresponding to the key/value if exists with ordinal re-cycling.
   *
   * The ordinal corresponding to the given key may be re-assigned to another tuple. It is
   * important that consumer checks the ordinal value via
   * {@link MultiMapWithOrdinal#getOrdinal(Object)} before attempting to look-up by ordinal.
   *
   * If the multimap is changed return true.
   */
  @Override
  public synchronized boolean remove(K key, V value) {
    Set<Integer> removalSet = getOrdinals(key);
    if (removalSet.isEmpty()) {
      return false;
    }
    Optional<V> removeValue = removalSet.stream().map(ordinalToValue::get).filter(value::equals).findFirst();
    if (!removeValue.isPresent()) {
      return false;
    }
    int removalOrdinal = removeKv(removalSet, key, value);
    int lastOrdinal = ordinalToValue.size();
    if (lastOrdinal != removalOrdinal) { //we didn't remove the last ordinal
      swapOrdinal(lastOrdinal, removalOrdinal);
    }
    return true;
  }

  private void swapOrdinal(int lastOrdinal, int removalOrdinal) {
    V swapOrdinalValue = ordinalToValue.remove(lastOrdinal);
    ordinalToValue.put(removalOrdinal, swapOrdinalValue);
    K swapOrdinalKey = keyToOrdinal.entrySet()
        .stream()
        .filter(kv -> kv.getValue().stream().anyMatch(o -> o == lastOrdinal))
        .map(Map.Entry::getKey)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("MultimapWithOrdinal in bad state"));
    ordinalToValue.put(removalOrdinal, swapOrdinalValue);
    Set<Integer> swapSet = getOrdinals(swapOrdinalKey);
    swapSet.remove(lastOrdinal);
    swapSet.add(removalOrdinal);
    keyToOrdinal.put(swapOrdinalKey, swapSet);
  }

  private int removeKv(Set<Integer> removalSet, K key, V value) {
    Integer removalOrdinal = removalSet.stream()
        .filter(i -> ordinalToValue.get(i).equals(value))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("MultimapWithOrdinal in bad state"));
    ordinalToValue.remove(removalOrdinal);
    removalSet.remove(removalOrdinal);
    if (removalSet.isEmpty()) {
      keyToOrdinal.remove(key);
    } else {
      keyToOrdinal.put(key, removalSet);
    }
    return removalOrdinal;
  }

  /**
   * remove all entries of key.
   */
  @Override
  public synchronized boolean removeAll(K key) {
    Collection<V> values = this.getAll(key);
    if (values == null) {
      return false;
    }
    for (V v: values) {
      this.remove(key, v);
    }
    return true;
  }

  @Override
  public void clear() {
    ordinalToValue.clear();
    keyToOrdinal.clear();
  }

  @Override
  public Set<K> keys() {
    return keyToOrdinal.keySet();
  }

}

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
import java.util.Set;

import org.apache.arrow.vector.complex.AbstractStructVector;

/**
 * Implementation of MapWithOrdinal that allows for promotion to multimap when duplicate fields exist.
 * @param <K> key type
 * @param <V> value type
 */
public class PromotableMultiMapWithOrdinal<K, V> implements MapWithOrdinal<K, V> {
  private final MapWithOrdinalImpl<K, V> mapWithOrdinal = new MapWithOrdinalImpl<>();
  private final MultiMapWithOrdinal<K, V> multiMapWithOrdinal = new MultiMapWithOrdinal<>();
  private final boolean promotable;
  private AbstractStructVector.ConflictPolicy conflictPolicy;
  private MapWithOrdinal<K, V> delegate;

  /**
   * Create promotable map.
   * @param promotable if promotion is allowed, otherwise delegate to MapWithOrdinal.
   * @param conflictPolicy how to handle name conflicts.
   */
  public PromotableMultiMapWithOrdinal(boolean promotable, AbstractStructVector.ConflictPolicy conflictPolicy) {
    this.promotable = promotable;
    this.conflictPolicy = conflictPolicy;
    delegate = mapWithOrdinal;
  }

  private void promote() {
    if (delegate == multiMapWithOrdinal ||
        !promotable ||
        conflictPolicy.equals(AbstractStructVector.ConflictPolicy.CONFLICT_REPLACE)) {
      return;
    }
    for (K key : mapWithOrdinal.keys()) {
      V value = mapWithOrdinal.get(key);
      multiMapWithOrdinal.put(key, value, false);
    }
    mapWithOrdinal.clear();
    delegate = multiMapWithOrdinal;
  }

  @Override
  public V getByOrdinal(int id) {
    return delegate.getByOrdinal(id);
  }

  @Override
  public int getOrdinal(K key) {
    return delegate.getOrdinal(key);
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
  public V get(K key) {
    return delegate.get(key);
  }

  @Override
  public Collection<V> getAll(K key) {
    return delegate.getAll(key);
  }

  @Override
  public boolean put(K key, V value, boolean overwrite) {
    if (delegate.containsKey(key)) {
      promote();
    }
    return delegate.put(key, value, overwrite);
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public boolean containsKey(K key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean remove(K key, V value) {
    return delegate.remove(key, value);
  }

  @Override
  public boolean removeAll(K key) {
    return delegate.removeAll(key);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public Set<K> keys() {
    return delegate.keys();
  }

  public void setConflictPolicy(AbstractStructVector.ConflictPolicy conflictPolicy) {
    this.conflictPolicy = conflictPolicy;
  }
}

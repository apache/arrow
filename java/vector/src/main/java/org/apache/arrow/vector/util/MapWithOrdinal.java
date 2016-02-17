/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.util;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of map that supports constant time look-up by a generic key or an ordinal.
 *
 * This class extends the functionality a regular {@link Map} with ordinal lookup support.
 * Upon insertion an unused ordinal is assigned to the inserted (key, value) tuple.
 * Upon update the same ordinal id is re-used while value is replaced.
 * Upon deletion of an existing item, its corresponding ordinal is recycled and could be used by another item.
 *
 * For any instance with N items, this implementation guarantees that ordinals are in the range of [0, N). However,
 * the ordinal assignment is dynamic and may change after an insertion or deletion. Consumers of this class are
 * responsible for explicitly checking the ordinal corresponding to a key via
 * {@link org.apache.arrow.vector.util.MapWithOrdinal#getOrdinal(Object)} before attempting to execute a lookup
 * with an ordinal.
 *
 * @param <K> key type
 * @param <V> value type
 */

public class MapWithOrdinal<K, V> implements Map<K, V> {
  private final static Logger logger = LoggerFactory.getLogger(MapWithOrdinal.class);

  private final Map<K, Entry<Integer, V>> primary = Maps.newLinkedHashMap();
  private final IntObjectHashMap<V> secondary = new IntObjectHashMap<>();

  private final Map<K, V> delegate = new Map<K, V>() {
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
      final int ordinal = oldPair == null ? primary.size():oldPair.getKey();
      primary.put(key, new AbstractMap.SimpleImmutableEntry<>(ordinal, value));
      secondary.put(ordinal, value);
      return oldPair==null ? null:oldPair.getValue();
    }

    @Override
    public V remove(Object key) {
      final Entry<Integer, V> oldPair = primary.remove(key);
      if (oldPair!=null) {
        final int lastOrdinal = secondary.size();
        final V last = secondary.get(lastOrdinal);
        // normalize mappings so that all numbers until primary.size() is assigned
        // swap the last element with the deleted one
        secondary.put(oldPair.getKey(), last);
        primary.put((K) key, new AbstractMap.SimpleImmutableEntry<>(oldPair.getKey(), last));
      }
      return oldPair==null ? null:oldPair.getValue();
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
      return Lists.newArrayList(Iterables.transform(secondary.entries(), new Function<IntObjectMap.Entry<V>, V>() {
        @Override
        public V apply(IntObjectMap.Entry<V> entry) {
          return Preconditions.checkNotNull(entry).value();
        }
      }));
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
      return Sets.newHashSet(Iterables.transform(primary.entrySet(), new Function<Entry<K, Entry<Integer, V>>, Entry<K, V>>() {
        @Override
        public Entry<K, V> apply(Entry<K, Entry<Integer, V>> entry) {
          return new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue().getValue());
        }
      }));
    }
  };

  /**
   * Returns the value corresponding to the given ordinal
   *
   * @param id ordinal value for lookup
   * @return an instance of V
   */
  public V getByOrdinal(int id) {
    return secondary.get(id);
  }

  /**
   * Returns the ordinal corresponding to the given key.
   *
   * @param key key for ordinal lookup
   * @return ordinal value corresponding to key if it exists or -1
   */
  public int getOrdinal(K key) {
    Entry<Integer, V> pair = primary.get(key);
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
  public V get(Object key) {
    return delegate.get(key);
  }

  /**
   * Inserts the tuple (key, value) into the map extending the semantics of {@link Map#put} with automatic ordinal
   * assignment. A new ordinal is assigned if key does not exists. Otherwise the same ordinal is re-used but the value
   * is replaced.
   *
   * {@see java.util.Map#put}
   */
  @Override
  public V put(K key, V value) {
    return delegate.put(key, value);
  }

  @Override
  public Collection<V> values() {
    return delegate.values();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate.containsValue(value);
  }

  /**
   * Removes the element corresponding to the key if exists extending the semantics of {@link Map#remove} with ordinal
   * re-cycling. The ordinal corresponding to the given key may be re-assigned to another tuple. It is important that
   * consumer checks the ordinal value via {@link #getOrdinal(Object)} before attempting to look-up by ordinal.
   *
   * {@see java.util.Map#remove}
   */
  @Override
  public V remove(Object key) {
    return delegate.remove(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    delegate.putAll(m);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public Set<K> keySet() {
    return delegate.keySet();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate.entrySet();
  }
}

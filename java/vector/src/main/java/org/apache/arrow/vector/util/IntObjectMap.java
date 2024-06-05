/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.arrow.vector.util;

import java.util.Iterator;
import java.util.Map;

/**
 * A vendored specialized copy of Netty's IntObjectMap for use within Arrow.
 * Avoids requiring Netty in the Arrow core just for this one class.
 *
 * @param <V> the value type stored in the map.
 */
interface IntObjectMap<V> extends Map<Integer, V> {

  /**
   * A primitive entry in the map, provided by the iterator from {@link #entries()}.
   *
   * @param <V> the value type stored in the map.
   */
  interface PrimitiveEntry<V> {
    /**
     * Gets the key for this entry.
     */
    int key();

    /**
     * Gets the value for this entry.
     */
    V value();

    /**
     * Sets the value for this entry.
     */
    void setValue(V value);
  }

  /**
   * Gets the value in the map with the specified key.
   *
   * @param key the key whose associated value is to be returned.
   * @return the value or {@code null} if the key was not found in the map.
   */
  V get(int key);

  /**
   * Puts the given entry into the map.
   *
   * @param key   the key of the entry.
   * @param value the value of the entry.
   * @return the previous value for this key or {@code null} if there was no previous mapping.
   */
  V put(int key, V value);

  /**
   * Removes the entry with the specified key.
   *
   * @param key the key for the entry to be removed from this map.
   * @return the previous value for the key, or {@code null} if there was no mapping.
   */
  V remove(int key);

  /**
   * Gets an iterable to traverse over the primitive entries contained in this map. As an optimization,
   * the {@link PrimitiveEntry}s returned by the {@link Iterator} may change as the {@link Iterator}
   * progresses. The caller should not rely on {@link PrimitiveEntry} key/value stability.
   */
  Iterable<PrimitiveEntry<V>> entries();

  /**
   * Indicates whether or not this map contains a value for the specified key.
   */
  boolean containsKey(int key);
}

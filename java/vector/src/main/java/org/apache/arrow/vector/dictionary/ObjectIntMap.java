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

/**
 * Specific hash map for int type value, reducing boxing/unboxing operations.
 * @param <K> key type.
 */
public interface ObjectIntMap<K> {

  /**
   * Associates the specified value with the specified key in this map.
   * If the map previously contained a mapping for the key, the old
   * value is replaced.
   * @param key key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with <tt>key</tt>, or
   *         -1 if there was no mapping for <tt>key</tt>.
   */
  int put(K key, int value);

  /**
   * Returns the value to which the specified key is mapped,
   * or -1 if this map contains no mapping for the key.
   */
  int get(K key);

  /**
   * Removes the mapping for the specified key from this map if present.
   * @param key key whose mapping is to be removed from the map
   * @return the previous value associated with <tt>key</tt>, or
   *         -1 if there was no mapping for <tt>key</tt>.
   */
  int remove(K key);
}

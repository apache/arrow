/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.arrow.memory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Highly specialized IdentityHashMap that implements only partial
 * Map APIs
 * It incurs low initial cost (just two elements by default)
 * It assumes Value includes the Key - Implements @ValueWithKeyIncluded iface
 * that provides "getKey" method
 * @param <V>
 */
public class LowCostIdentityHashMap<K, V extends ValueWithKeyIncluded<K>> {

  /*
   * The internal data structure to hold values.
   */
  private Object[] elementData;

  /* Actual number of values. */
  private int size;

  /*
   * maximum number of elements that can be put in this map before having to
   * rehash.
   */
  private int threshold;

  private static final int DEFAULT_MIN_SIZE = 1;

  /* Default load factor of 0.75; */
  private static final int loadFactor = 7500;

  /**
   * Creates an Map with default expected maximum size.
   */
  public LowCostIdentityHashMap() {
    this(DEFAULT_MIN_SIZE);
  }

  /**
   * Creates an Map with the specified maximum size parameter.
   *
   * @param maxSize
   *            The estimated maximum number of entries that will be put in
   *            this map.
   */
  public LowCostIdentityHashMap(int maxSize) {
    if (maxSize >= 0) {
      this.size = 0;
      threshold = getThreshold(maxSize);
      elementData = newElementArray(computeElementArraySize());
    } else {
      throw new IllegalArgumentException();
    }
  }

  private int getThreshold(int maxSize) {
    // assign the threshold to maxSize initially, this will change to a
    // higher value if rehashing occurs.
    return maxSize > 2 ? maxSize : 2;
  }

  private int computeElementArraySize() {
    int arraySize = (int) (((long) threshold * 10000) / loadFactor);
    // ensure arraySize is positive, the above cast from long to int type
    // leads to overflow and negative arraySize if threshold is too big
    return arraySize < 0 ? -arraySize : arraySize;
  }

  /**
   * Create a new element array
   *
   * @param s
   *            the number of elements
   * @return Reference to the element array
   */
  private Object[] newElementArray(int s) {
    return new Object[s];
  }

  /**
   * Removes all elements from this map, leaving it empty.
   *
   * @see #isEmpty()
   * @see #size()
   */
  public void clear() {
    size = 0;
    for (int i = 0; i < elementData.length; i++) {
      elementData[i] = null;
    }
  }

  /**
   * Returns whether this map contains the specified key.
   *
   * @param key
   *            the key to search for.
   * @return {@code true} if this map contains the specified key,
   *         {@code false} otherwise.
   */
  public boolean containsKey(K key) {
    Preconditions.checkNotNull(key);

    int index = findIndex(key, elementData);
    return (elementData[index] == null) ? false : ((V)elementData[index]).getKey() == key;
  }

  /**
   * Returns whether this map contains the specified value.
   *
   * @param value
   *            the value to search for.
   * @return {@code true} if this map contains the specified value,
   *         {@code false} otherwise.
   */
  public boolean containsValue(V value) {
    Preconditions.checkNotNull(value);

    for (int i = 0; i < elementData.length; i++) {
      if (elementData[i] == value) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the value of the mapping with the specified key.
   *
   * @param key
   *            the key.
   * @return the value of the mapping with the specified key.
   */
  public V get(K key) {
    Preconditions.checkNotNull(key);

    int index = findIndex(key, elementData);

    return (elementData[index] == null) ? null :
      (((V)elementData[index]).getKey() == key) ? (V)elementData[index] : null;
  }

  /**
   * Returns the index where the key is found at, or the index of the next
   * empty spot if the key is not found in this table.
   */
  @VisibleForTesting
  int findIndex(Object key, Object[] array) {
    int length = array.length;
    int index = getModuloHash(key, length);
    int last = (index + length - 1) % length;
    while (index != last) {
      if ((array[index] == null) || ((V)array[index]).getKey() == key) {
        /*
         * Found the key, or the next empty spot (which means key is not
         * in the table)
         */
        break;
      }
      index = (index + 1) % length;
    }
    return index;
  }

  @VisibleForTesting
  static int getModuloHash(Object key, int length) {
    return ((System.identityHashCode(key) & 0x7FFFFFFF) % length);
  }

  /**
   * Maps the specified key to the specified value.
   *
   * @param value
   *            the value.
   * @return the value of any previous mapping with the specified key or
   *         {@code null} if there was no such mapping.
   */
  public V put(V value) {
    Preconditions.checkNotNull(value);
    K _key = value.getKey();
    Preconditions.checkNotNull(_key);
    V _value = value;

    int index = findIndex(_key, elementData);

    // if the key doesn't exist in the table
    if (elementData[index] == null || ((V)elementData[index]).getKey() != _key) {
      if (++size > threshold) {
        rehash();
        index = findIndex(_key, elementData);
      }

      // insert the key and assign the value to null initially
      elementData[index] = null;
    }

    // insert value to where it needs to go, return the old value
    Object result = elementData[index];
    elementData[index] = _value;

    return (V) result;
  }

  @VisibleForTesting
  void rehash() {
    int newlength = elementData.length * 15 / 10;
    if (newlength == 0) {
      newlength = 1;
    }
    Object[] newData = newElementArray(newlength);
    for (int i = 0; i < elementData.length; i++) {
      Object key = (elementData[i] == null) ? null : ((V)elementData[i]).getKey();
      if (key != null) {
        // if not empty
        int index = findIndex(key, newData);
        newData[index] = elementData[i];
      }
    }
    elementData = newData;
    computeMaxSize();
  }

  private void computeMaxSize() {
    threshold = (int) ((long) (elementData.length) * loadFactor / 10000);
  }

  /**
   * Removes the mapping with the specified key from this map.
   *
   * @param key
   *            the key of the mapping to remove.
   * @return the value of the removed mapping, or {@code null} if no mapping
   *         for the specified key was found.
   */
  public V remove(K key) {
    Preconditions.checkNotNull(key);

    boolean hashedOk;
    int index, next, hash;
    Object result, object;
    index = next = findIndex(key, elementData);

    if (elementData[index] == null || ((V)elementData[index]).getKey() != key) {
      return null;
    }

    // store the value for this key
    result = elementData[index];
    // clear value to allow movement of the rest of the elements
    elementData[index] = null;
    size--;

    // shift the following elements up if needed
    // until we reach an empty spot
    int length = elementData.length;
    while (true) {
      next = (next + 1) % length;
      object = elementData[next];
      if (object == null) {
        break;
      }

      hash = getModuloHash(((V)object).getKey(), length);
      hashedOk = hash > index;
      if (next < index) {
        hashedOk = hashedOk || (hash <= next);
      } else {
        hashedOk = hashedOk && (hash <= next);
      }
      if (!hashedOk) {
        elementData[index] = object;
        index = next;
        elementData[index] = null;
      }
    }

    return (V) result;
  }



  /**
   * Returns whether this Map has no elements.
   *
   * @return {@code true} if this Map has no elements,
   *         {@code false} otherwise.
   * @see #size()
   */
  public boolean isEmpty() {
    return size == 0;
  }

  /**
   * Returns the number of mappings in this Map.
   *
   * @return the number of mappings in this Map.
   */
  public int size() {
    return size;
  }

  /**
   * Special API to return next value - substitute of regular Map.values.iterator().next()
   * @return next available value or null if none available
   */
  public V getNextValue() {
    for (int i = 0; i < elementData.length; i++) {
      if (elementData[i] != null) {
        return (V)elementData[i];
      }
    }
    return null;
  }
}

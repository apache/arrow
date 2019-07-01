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

package org.apache.arrow.memory;

import java.util.Objects;

/**
 * Implementation of the {@link ObjectIntMap} interface, used for DictionaryEncoder.
 * Note that value in this map is always not less than 0, and -1 represents a null value.
 */
public class DictionaryEncodeHashMap<K> implements ObjectIntMap<K> {

  static final int NULL_VALUE = -1;

  static final int DEFAULT_INITIAL_CAPACITY = 1 << 4;

  static final int MAXIMUM_CAPACITY = 1 << 30;

  static final float DEFAULT_LOAD_FACTOR = 0.75f;

  static final Entry<?>[] EMPTY_TABLE = {};

  transient Entry<K>[] table = (Entry<K>[]) EMPTY_TABLE;

  transient int size;

  int threshold;

  final float loadFactor;

  public DictionaryEncodeHashMap(int initialCapacity, float loadFactor) {
    if (initialCapacity < 0)
      throw new IllegalArgumentException("Illegal initial capacity: " +
          initialCapacity);
    if (initialCapacity > MAXIMUM_CAPACITY)
      initialCapacity = MAXIMUM_CAPACITY;
    if (loadFactor <= 0 || Float.isNaN(loadFactor))
      throw new IllegalArgumentException("Illegal load factor: " +
          loadFactor);
    this.loadFactor = loadFactor;
    this.threshold = initialCapacity;
  }

  public DictionaryEncodeHashMap(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public DictionaryEncodeHashMap() {
    this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
  }

  private void inflateTable(int threshold){
    int capacity = roundUpToPowerOf2(threshold);
    this.threshold = (int) Math.min(capacity * loadFactor, MAXIMUM_CAPACITY + 1);
    table = new Entry[capacity];
  }

  static final int hash(Object key) {
    int h;
    return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
  }

  static int indexFor(int h, int length) {
    return h & (length-1);
  }


  static final int roundUpToPowerOf2(int size) {
    int n = size - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }

  @Override
  public int get(K key) {
    int hash = hash(key);
    int index = indexFor(hash, table.length);
    for (Entry<K> e = table[index] ; e != null ; e = e.next) {
      if ((e.hash == hash) && e.key.equals(key)) {
        return e.value;
      }
    }
    return NULL_VALUE;
  }

  @Override
  public int put(K key, int value) {
    if (table == EMPTY_TABLE) {
      inflateTable(threshold);
    }

    if (key == null) {
      return putForNullKey(value);
    }

    int hash = hash(key);
    int i = indexFor(hash, table.length);
    for (Entry<K> e = table[i]; e != null; e = e.next) {
      Object k;
      if (e.hash == hash && ((k = e.key) == key || key.equals(k))) {
        int oldValue = e.value;
        e.value = value;
        return oldValue;
      }
    }

    addEntry(hash, key, value, i);
    return NULL_VALUE;
  }


  @Override
  public int remove(K key) {
    Entry<K> e = removeEntryForKey(key);
    return (e == null ? NULL_VALUE : e.value);
  }

  void createEntry(int hash, K key, int value, int bucketIndex) {
    Entry<K> e = table[bucketIndex];
    table[bucketIndex] = new Entry<>(hash, key, value, e);
    size++;
  }

  private int putForNullKey(int value) {
    for (Entry<K> e = table[0]; e != null; e = e.next) {
      if (e.key == null) {
        int oldValue = e.value;
        e.value = value;
        return oldValue;
      }
    }
    addEntry(0, null, value, 0);
    return NULL_VALUE;
  }

  void addEntry(int hash, K key, int value, int bucketIndex) {
    if ((size >= threshold) && (null != table[bucketIndex])) {
      resize(2 * table.length);
      hash = (null != key) ? hash(key) : 0;
      bucketIndex = indexFor(hash, table.length);
    }

    createEntry(hash, key, value, bucketIndex);
  }

  void resize(int newCapacity) {
    Entry[] oldTable = table;
    int oldCapacity = oldTable.length;
    if (oldCapacity == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    Entry[] newTable = new Entry[newCapacity];
    transfer(newTable);
    table = newTable;
    threshold = (int)Math.min(newCapacity * loadFactor, MAXIMUM_CAPACITY + 1);
  }

  void transfer(Entry[] newTable) {
    int newCapacity = newTable.length;
    for (Entry<K> e : table) {
      while(null != e) {
        Entry<K> next = e.next;
        int i = indexFor(e.hash, newCapacity);
        e.next = newTable[i];
        newTable[i] = e;
        e = next;
      }
    }
  }

  final Entry<K> removeEntryForKey(Object key) {
    if (size == 0) {
      return null;
    }
    int hash = (key == null) ? 0 : hash(key);
    int i = indexFor(hash, table.length);
    Entry<K> prev = table[i];
    Entry<K> e = prev;

    while (e != null) {
      Entry<K> next = e.next;
      Object k;
      if (e.hash == hash && ((k = e.key) == key || (key != null && key.equals(k)))) {
        size--;
        if (prev == e) {
          table[i] = next;
        } else {
          prev.next = next;
        }

        return e;
      }
      prev = e;
      e = next;
    }

    return e;
  }

  /**
   * Returns the number of mappings in this Map.
   */
  public int size() {
    return size;
  }

  public void clear() {
    size = 0;
    for (int i = 0; i < table.length; i++) {
      table[i] = null;
    }
  }

  static class Entry<K> {
    final K key;
    int value;
    Entry<K> next;
    int hash;

    Entry(int hash, K key, int value, Entry<K> next) {
      this.key = key;
      this.value = value;
      this.hash = hash;
      this.next = next;
    }

    public final K getKey() {
      return key;
    }

    public final int getValue() {
      return value;
    }

    public final int setValue(int newValue) {
      int oldValue = value;
      value = newValue;
      return oldValue;
    }

    public final boolean equals(Object o) {
      if (!(o instanceof DictionaryEncodeHashMap.Entry)) {
        return false;
      }
      Entry e = (Entry) o;
      if (Objects.equals(key, e.getKey())) {
        if (value == e.getValue()) {
          return true;
        }
      }
      return false;
    }

    public final int hashCode() {
      return Objects.hashCode(key) ^ Objects.hashCode(value);
    }

    public final String toString() {
      return getKey() + "=" + getValue();
    }
  }

}

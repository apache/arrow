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

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;

/**
 * HashTable used for Dictionary encoding. It holds two vectors (the vector to encode and dictionary vector)
 * It stores the index in dictionary vector and for a given index in encode vector,
 * it could return dictionary index.
 */
public class DictionaryHashTable {

  /**
   * Represents a null value in map.
   */
  static final int NULL_VALUE = -1;

  /**
   * The default initial capacity - MUST be a power of two.
   */
  static final int DEFAULT_INITIAL_CAPACITY = 1 << 4;

  /**
   * The maximum capacity, used if a higher value is implicitly specified
   * by either of the constructors with arguments.
   */
  static final int MAXIMUM_CAPACITY = 1 << 30;

  /**
   * The load factor used when none specified in constructor.
   */
  static final float DEFAULT_LOAD_FACTOR = 0.75f;

  static final DictionaryHashTable.Entry[] EMPTY_TABLE = {};

  /**
   * The table, initialized on first use, and resized as
   * necessary. When allocated, length is always a power of two.
   */
  transient DictionaryHashTable.Entry[] table = EMPTY_TABLE;

  /**
   * The number of key-value mappings contained in this map.
   */
  transient int size;

  /**
   * The next size value at which to resize (capacity * load factor).
   */
  int threshold;

  /**
   * The load factor for the hash table.
   */
  final float loadFactor;

  private final ValueVector dictionary;

  /**
   * Constructs an empty map with the specified initial capacity and load factor.
   */
  public DictionaryHashTable(int initialCapacity, ValueVector dictionary) {
    if (initialCapacity < 0) {
      throw new IllegalArgumentException("Illegal initial capacity: " +
          initialCapacity);
    }
    if (initialCapacity > MAXIMUM_CAPACITY) {
      initialCapacity = MAXIMUM_CAPACITY;
    }
    this.loadFactor = DEFAULT_LOAD_FACTOR;
    this.threshold = initialCapacity;

    this.dictionary = dictionary;

    // build hash table
    for (int i = 0; i < this.dictionary.getValueCount(); i++) {
      put(i);
    }
  }

  public DictionaryHashTable(ValueVector dictionary) {
    this(DEFAULT_INITIAL_CAPACITY, dictionary);
  }

  /**
   * Compute the capacity with given threshold and create init table.
   */
  private void inflateTable(int threshold) {
    int capacity = roundUpToPowerOf2(threshold);
    this.threshold = (int) Math.min(capacity * loadFactor, MAXIMUM_CAPACITY + 1);
    table = new DictionaryHashTable.Entry[capacity];
  }

  /**
   * Computes the storage location in an array for the given hashCode.
   */
  static int indexFor(int h, int length) {
    return h & (length - 1);
  }

  /**
   * Returns a power of two size for the given size.
   */
  static final int roundUpToPowerOf2(int size) {
    int n = size - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }

  /**
   * get the corresponding dictionary index with the given index in vector which to encode.
   * @param indexInArray index in vector.
   * @return dictionary vector index or -1 if no value equals.
   */
  public int getIndex(int indexInArray, ValueVector toEncode) {
    int hash = toEncode.hashCode(indexInArray);
    int index = indexFor(hash, table.length);

    RangeEqualsVisitor equalVisitor = new RangeEqualsVisitor(dictionary, toEncode, false);
    Range range = new Range(0, 0, 1);

    for (DictionaryHashTable.Entry e = table[index]; e != null ; e = e.next) {
      if (e.hash == hash) {
        int dictIndex = e.index;

        range = range.setRightStart(indexInArray)
            .setLeftStart(dictIndex);
        if (equalVisitor.rangeEquals(range)) {
          return dictIndex;
        }
      }
    }
    return NULL_VALUE;
  }

  /**
   * put the index of dictionary vector to build hash table.
   */
  private void put(int indexInDictionary) {
    if (table == EMPTY_TABLE) {
      inflateTable(threshold);
    }

    int hash = dictionary.hashCode(indexInDictionary);
    int i = indexFor(hash, table.length);
    for (DictionaryHashTable.Entry e = table[i]; e != null; e = e.next) {
      if (e.hash == hash && e.index == indexInDictionary) {
        //already has this index, return
        return;
      }
    }

    addEntry(hash, indexInDictionary, i);
  }

  /**
   * Create a new Entry at the specific position of table.
   */
  void createEntry(int hash, int index, int bucketIndex) {
    DictionaryHashTable.Entry e = table[bucketIndex];
    table[bucketIndex] = new DictionaryHashTable.Entry(hash, index, e);
    size++;
  }

  /**
   * Add Entry at the specified location of the table.
   */
  void addEntry(int hash, int index, int bucketIndex) {
    if ((size >= threshold) && (null != table[bucketIndex])) {
      resize(2 * table.length);
      bucketIndex = indexFor(hash, table.length);
    }

    createEntry(hash, index, bucketIndex);
  }

  /**
   * Resize table with given new capacity.
   */
  void resize(int newCapacity) {
    DictionaryHashTable.Entry[] oldTable = table;
    int oldCapacity = oldTable.length;
    if (oldCapacity == MAXIMUM_CAPACITY) {
      threshold = Integer.MAX_VALUE;
      return;
    }

    DictionaryHashTable.Entry[] newTable = new DictionaryHashTable.Entry[newCapacity];
    transfer(newTable);
    table = newTable;
    threshold = (int) Math.min(newCapacity * loadFactor, MAXIMUM_CAPACITY + 1);
  }

  /**
   * Transfer entries into new table from old table.
   * @param newTable new table
   */
  void transfer(DictionaryHashTable.Entry[] newTable) {
    int newCapacity = newTable.length;
    for (DictionaryHashTable.Entry e : table) {
      while (null != e) {
        DictionaryHashTable.Entry next = e.next;
        int i = indexFor(e.hash, newCapacity);
        e.next = newTable[i];
        newTable[i] = e;
        e = next;
      }
    }
  }

  /**
   * Returns the number of mappings in this Map.
   */
  public int size() {
    return size;
  }

  /**
   * Removes all elements from this map, leaving it empty.
   */
  public void clear() {
    size = 0;
    for (int i = 0; i < table.length; i++) {
      table[i] = null;
    }
  }

  /**
   * Class to keep dictionary index data within hash table.
   */
  static class Entry {
    //dictionary index
    int index;
    DictionaryHashTable.Entry next;
    int hash;

    Entry(int hash, int index, DictionaryHashTable.Entry next) {
      this.index = index;
      this.hash = hash;
      this.next = next;
    }

    public final int getIndex() {
      return this.index;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    public final boolean equals(Object o) {
      if (!(o instanceof DictionaryHashTable.Entry)) {
        return false;
      }
      DictionaryHashTable.Entry e = (DictionaryHashTable.Entry) o;
      if (index == e.getIndex()) {
        return true;
      }
      return false;
    }
  }
}

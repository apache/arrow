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

package org.apache.arrow.algorithm.dictionary;

import java.util.HashMap;

import org.apache.arrow.memory.util.ArrowBufPointer;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.memory.util.hash.SimpleHasher;
import org.apache.arrow.vector.ElementAddressableVector;

/**
 * This class builds the dictionary based on a hash table.
 * Each add operation can be finished in O(1) time,
 * where n is the current dictionary size.
 *
 * @param <V> the dictionary vector type.
 */
public class HashTableBasedDictionaryBuilder<V extends ElementAddressableVector> implements DictionaryBuilder<V> {

  /**
   * The dictionary to be built.
   */
  private final V dictionary;

  /**
   * If null should be encoded.
   */
  private final boolean encodeNull;

  /**
   * The hash map for distinct dictionary entries.
   * The key is the pointer to the dictionary element, whereas the value is the index in the dictionary.
   */
  private HashMap<ArrowBufPointer, Integer> hashMap = new HashMap<>();

  /**
   * The hasher used for calculating the hash code.
   */
  private final ArrowBufHasher hasher;

  /**
   * Next pointer to try to add to the hash table.
   */
  private ArrowBufPointer nextPointer;

  /**
   * Constructs a hash table based dictionary builder.
   *
   * @param dictionary the dictionary to populate.
   */
  public HashTableBasedDictionaryBuilder(V dictionary) {
    this(dictionary, false);
  }

  /**
   * Constructs a hash table based dictionary builder.
   *
   * @param dictionary the dictionary to populate.
   * @param encodeNull if null values should be added to the dictionary.
   */
  public HashTableBasedDictionaryBuilder(V dictionary, boolean encodeNull) {
    this(dictionary, encodeNull, SimpleHasher.INSTANCE);
  }

  /**
   * Constructs a hash table based dictionary builder.
   *
   * @param dictionary the dictionary to populate.
   * @param encodeNull if null values should be added to the dictionary.
   * @param hasher     the hasher used to compute the hash code.
   */
  public HashTableBasedDictionaryBuilder(V dictionary, boolean encodeNull, ArrowBufHasher hasher) {
    this.dictionary = dictionary;
    this.encodeNull = encodeNull;
    this.hasher = hasher;
    this.nextPointer = new ArrowBufPointer(hasher);
  }

  /**
   * Gets the dictionary built.
   *
   * @return the dictionary.
   */
  @Override
  public V getDictionary() {
    return dictionary;
  }

  /**
   * Try to add all values from the target vector to the dictionary.
   *
   * @param targetVector the target vector containing values to probe.
   * @return the number of values actually added to the dictionary.
   */
  @Override
  public int addValues(V targetVector) {
    int oldDictSize = dictionary.getValueCount();
    for (int i = 0; i < targetVector.getValueCount(); i++) {
      if (!encodeNull && targetVector.isNull(i)) {
        continue;
      }
      addValue(targetVector, i);
    }

    return dictionary.getValueCount() - oldDictSize;
  }

  /**
   * Try to add an element from the target vector to the dictionary.
   *
   * @param targetVector the target vector containing new element.
   * @param targetIndex  the index of the new element in the target vector.
   * @return the index of the new element in the dictionary.
   */
  @Override
  public int addValue(V targetVector, int targetIndex) {
    targetVector.getDataPointer(targetIndex, nextPointer);

    Integer index = hashMap.get(nextPointer);
    if (index == null) {
      // a new dictionary element is found

      // insert it to the dictionary
      int dictSize = dictionary.getValueCount();
      dictionary.copyFromSafe(targetIndex, dictSize, targetVector);
      dictionary.setValueCount(dictSize + 1);
      dictionary.getDataPointer(dictSize, nextPointer);

      // insert it to the hash map
      hashMap.put(nextPointer, dictSize);
      nextPointer = new ArrowBufPointer(hasher);

      return dictSize;
    }
    return index;
  }
}

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
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ElementAddressableVector;

/**
 * Dictionary encoder based on hash table.
 * @param <E> encoded vector type.
 * @param <D> decoded vector type, which is also the dictionary type.
 */
public class HashTableDictionaryEncoder<E extends BaseIntVector, D extends ElementAddressableVector>
    implements DictionaryEncoder<E, D> {

  /**
   * The dictionary for encoding/decoding.
   * It must be sorted.
   */
  private final D dictionary;

  /**
   * The hasher used to compute the hash code.
   */
  private final ArrowBufHasher hasher;

  /**
   * A flag indicating if null should be encoded.
   */
  private final boolean encodeNull;

  /**
   * The hash map for distinct dictionary entries.
   * The key is the pointer to the dictionary element, whereas the value is the index in the dictionary.
   */
  private HashMap<ArrowBufPointer, Integer> hashMap = new HashMap<>();

  /**
   * The pointer used to probe each element to encode.
   */
  private ArrowBufPointer reusablePointer;

  /**
   * Constructs a dictionary encoder.
   * @param dictionary the dictionary.
   *
   */
  public HashTableDictionaryEncoder(D dictionary) {
    this(dictionary, false);
  }

  /**
   * Constructs a dictionary encoder.
   * @param dictionary the dictionary.
   * @param encodeNull a flag indicating if null should be encoded.
   *     It determines the behaviors for processing null values in the input during encoding/decoding.
   *    <li>
   *       For encoding, when a null is encountered in the input,
   *       1) If the flag is set to true, the encoder searches for the value in the dictionary,
   *       and outputs the index in the dictionary.
   *       2) If the flag is set to false, the encoder simply produces a null in the output.
   *    </li>
   *    <li>
   *       For decoding, when a null is encountered in the input,
   *       1) If the flag is set to true, the decoder should never expect a null in the input.
   *       2) If set to false, the decoder simply produces a null in the output.
   *    </li>
   */
  public HashTableDictionaryEncoder(D dictionary, boolean encodeNull) {
    this(dictionary, encodeNull, SimpleHasher.INSTANCE);
  }

  /**
   * Constructs a dictionary encoder.
   * @param dictionary the dictionary.
   * @param encodeNull a flag indicating if null should be encoded.
    *     It determines the behaviors for processing null values in the input during encoding.
    *     When a null is encountered in the input,
    *     1) If the flag is set to true, the encoder searches for the value in the dictionary,
    *     and outputs the index in the dictionary.
    *     2) If the flag is set to false, the encoder simply produces a null in the output.
   * @param hasher the hasher used to calculate the hash code.
   */
  public HashTableDictionaryEncoder(D dictionary, boolean encodeNull, ArrowBufHasher hasher) {
    this.dictionary = dictionary;
    this.hasher = hasher;
    this.encodeNull = encodeNull;

    reusablePointer = new ArrowBufPointer(hasher);

    buildHashMap();
  }

  private void buildHashMap() {
    for (int i = 0; i < dictionary.getValueCount(); i++) {
      ArrowBufPointer pointer = new ArrowBufPointer(hasher);
      dictionary.getDataPointer(i, pointer);
      hashMap.put(pointer, i);
    }
  }

  /**
   * Encodes an input vector by a hash table.
   * So the algorithm takes O(n) time, where n is the length of the input vector.
   *
   * @param input  the input vector.
   * @param output the output vector.
   **/
  @Override
  public void encode(D input, E output) {
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!encodeNull && input.isNull(i)) {
        continue;
      }

      input.getDataPointer(i, reusablePointer);
      Integer index = hashMap.get(reusablePointer);

      if (index == null) {
        throw new IllegalArgumentException("The data element is not found in the dictionary");
      }
      output.setWithPossibleTruncate(i, index);
    }
    output.setValueCount(input.getValueCount());
  }
}

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

import org.apache.arrow.algorithm.search.VectorSearcher;
import org.apache.arrow.algorithm.sort.VectorValueComparator;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ValueVector;

/**
 * Dictionary encoder based on searching.
 * @param <E> encoded vector type.
 * @param <D> decoded vector type, which is also the dictionary type.
 */
public class SearchDictionaryEncoder<E extends BaseIntVector, D extends ValueVector>
    implements DictionaryEncoder<E, D> {

  /**
   * The dictionary for encoding/decoding.
   * It must be sorted.
   */
  private final D dictionary;

  /**
   * The criteria by which the dictionary is sorted.
   */
  private final VectorValueComparator<D> comparator;

  /**
   * A flag indicating if null should be encoded.
   */
  private final boolean encodeNull;

  /**
   * Constructs a dictionary encoder.
   * @param dictionary the dictionary. It must be in sorted order.
   * @param comparator the criteria for sorting.
   */
  public SearchDictionaryEncoder(D dictionary, VectorValueComparator<D> comparator) {
    this(dictionary, comparator, false);
  }

  /**
   * Constructs a dictionary encoder.
   * @param dictionary the dictionary. It must be in sorted order.
   * @param comparator the criteria for sorting.
   * @param encodeNull a flag indicating if null should be encoded.
   *     It determines the behaviors for processing null values in the input during encoding.
   *     When a null is encountered in the input,
   *     1) If the flag is set to true, the encoder searches for the value in the dictionary,
   *     and outputs the index in the dictionary.
   *     2) If the flag is set to false, the encoder simply produces a null in the output.
   */
  public SearchDictionaryEncoder(D dictionary, VectorValueComparator<D> comparator, boolean encodeNull) {
    this.dictionary = dictionary;
    this.comparator = comparator;
    this.encodeNull = encodeNull;
  }

  /**
   * Encodes an input vector by binary search.
   * So the algorithm takes O(n * log(m)) time, where n is the length of the input vector,
   * and m is the length of the dictionary.
   * @param input the input vector.
   * @param output the output vector. Note that it must be in a fresh state. At least,
   *     all its validity bits should be clear.
   */
  @Override
  public void encode(D input, E output) {
    for (int i = 0; i < input.getValueCount(); i++) {
      if (!encodeNull && input.isNull(i)) {
        // for this case, we should simply output a null in the output.
        // by assuming the output vector is fresh, we do nothing here.
        continue;
      }

      int index = VectorSearcher.binarySearch(dictionary, comparator, input, i);
      if (index == -1) {
        throw new IllegalArgumentException("The data element is not found in the dictionary: " + i);
      }
      output.setWithPossibleTruncate(i, index);
    }
    output.setValueCount(input.getValueCount());
  }
}

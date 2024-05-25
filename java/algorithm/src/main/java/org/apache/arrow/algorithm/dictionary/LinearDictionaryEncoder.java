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

import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.Range;
import org.apache.arrow.vector.compare.RangeEqualsVisitor;

/**
 * Dictionary encoder based on linear search.
 * @param <E> encoded vector type.
 * @param <D> decoded vector type, which is also the dictionary type.
 */
public class LinearDictionaryEncoder<E extends BaseIntVector, D extends ValueVector>
    implements DictionaryEncoder<E, D> {

  /**
   * The dictionary for encoding.
   */
  private final D dictionary;

  /**
   * A flag indicating if null should be encoded.
   */
  private final boolean encodeNull;

  private RangeEqualsVisitor equalizer;

  private Range range;

  /**
   * Constructs a dictionary encoder, with the encode null flag set to false.
   * @param dictionary the dictionary. Its entries should be sorted in the non-increasing order of their frequency.
   *     Otherwise, the encoder still produces correct results, but at the expense of performance overhead.
   */
  public LinearDictionaryEncoder(D dictionary) {
    this(dictionary, false);
  }

  /**
   * Constructs a dictionary encoder.
   * @param dictionary the dictionary. Its entries should be sorted in the non-increasing order of their frequency.
   *     Otherwise, the encoder still produces correct results, but at the expense of performance overhead.
   * @param encodeNull a flag indicating if null should be encoded.
   *     It determines the behaviors for processing null values in the input during encoding.
   *     When a null is encountered in the input,
   *     1) If the flag is set to true, the encoder searches for the value in the dictionary,
   *     and outputs the index in the dictionary.
   *     2) If the flag is set to false, the encoder simply produces a null in the output.
   */
  public LinearDictionaryEncoder(D dictionary, boolean encodeNull) {
    this.dictionary = dictionary;
    this.encodeNull = encodeNull;

    // temporarily set left and right vectors to dictionary
    equalizer = new RangeEqualsVisitor(dictionary, dictionary, null);
    range = new Range(0, 0, 1);
  }

  /**
   * Encodes an input vector by linear search.
   * When the dictionary is sorted in the non-increasing order of the entry frequency,
   * it will have constant time complexity, with no extra memory requirement.
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

      int index = linearSearch(input, i);
      if (index == -1) {
        throw new IllegalArgumentException("The data element is not found in the dictionary: " + i);
      }
      output.setWithPossibleTruncate(i, index);
    }
    output.setValueCount(input.getValueCount());
  }

  private int linearSearch(D input, int index) {
    range.setLeftStart(index);
    for (int i = 0; i < dictionary.getValueCount(); i++) {
      range.setRightStart(i);
      if (input.accept(equalizer, range)) {
        return i;
      }
    }
    return -1;
  }
}

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

/**
 * A dictionary encoder translates one vector into another one based on a dictionary vector.
 * According to Arrow specification, the encoded vector must be an integer based vector, which is
 * the index of the original vector element in the dictionary.
 *
 * @param <E> type of the encoded vector.
 * @param <D> type of the vector to encode. It is also the type of the dictionary vector.
 */
public interface DictionaryEncoder<E extends BaseIntVector, D extends ValueVector> {

  /**
   * Translates an input vector into an output vector.
   *
   * @param input the input vector.
   * @param output the output vector. Note that it must be in a fresh state. At least, all its
   *     validity bits should be clear.
   */
  void encode(D input, E output);
}

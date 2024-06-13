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

import org.apache.arrow.vector.ValueVector;

/**
 * A dictionary builder is intended for the scenario frequently encountered in practice: the
 * dictionary is not known a priori, so it is generated dynamically. In particular, when a new value
 * arrives, it is tested to check if it is already in the dictionary. If so, it is simply neglected,
 * otherwise, it is added to the dictionary.
 *
 * <p>The dictionary builder is intended to build a single dictionary. So it cannot be used for
 * different dictionaries.
 *
 * <p>Below gives the sample code for using the dictionary builder
 *
 * <pre>{@code
 * DictionaryBuilder dictionaryBuilder = ...
 * ...
 * dictionaryBuild.addValue(newValue);
 * ...
 * }</pre>
 *
 * <p>With the above code, the dictionary vector will be populated, and it can be retrieved by the
 * {@link DictionaryBuilder#getDictionary()} method. After that, dictionary encoding can proceed
 * with the populated dictionary..
 *
 * @param <V> the dictionary vector type.
 */
public interface DictionaryBuilder<V extends ValueVector> {

  /**
   * Try to add all values from the target vector to the dictionary.
   *
   * @param targetVector the target vector containing values to probe.
   * @return the number of values actually added to the dictionary.
   */
  int addValues(V targetVector);

  /**
   * Try to add an element from the target vector to the dictionary.
   *
   * @param targetVector the target vector containing new element.
   * @param targetIndex the index of the new element in the target vector.
   * @return the index of the new element in the dictionary.
   */
  int addValue(V targetVector, int targetIndex);

  /**
   * Gets the dictionary built.
   *
   * @return the dictionary.
   */
  V getDictionary();
}

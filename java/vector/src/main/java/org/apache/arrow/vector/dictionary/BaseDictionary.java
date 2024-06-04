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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

/**
 * Base interface for various dictionary implementations. Implementations include
 * {@link Dictionary} for encoding a complete vector and {@link BatchedDictionary}
 * for continuous encoding of a vector.
 * These methods provide means of accessing the dictionary vector containing the
 * encoded data.
 */
public interface BaseDictionary {

  /**
   * The dictionary vector containing unique entries.
   */
  FieldVector getVector();

  /**
   * The encoding used for the dictionary vector.
   */
  DictionaryEncoding getEncoding();

  /**
   * The type of the dictionary vector.
   */
  ArrowType getVectorType();

  /**
   * Mark the dictionary as complete for the batch. Called by the {@link ArrowWriter}
   * on {@link ArrowWriter#writeBatch()}.
   * @return The number of values written to the dictionary.
   */
  int mark();

  /**
   * Resets the dictionary to be used for a new batch. Called by the {@link ArrowWriter} on
   * {@link ArrowWriter#writeBatch()}.
   */
  void reset();

}

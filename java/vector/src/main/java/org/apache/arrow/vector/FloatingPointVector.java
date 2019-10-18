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

package org.apache.arrow.vector;

/**
 * The interface for vectors with floating point values.
 */
public interface FloatingPointVector {

  /**
   * Sets the value at the given index, note this value may be truncated internally.
   * @param index the index to set.
   * @param value the value to set.
   */
  void setWithPossibleTruncate(int index, double value);

  /**
   * Sets the value at the given index, note this value may be truncated internally.
   * Any expansion/reallocation is handled automatically.
   * @param index the index to set.
   * @param value the value to set.
   */
  void setSafeWithPossibleTruncate(int index, double value);

  /**
   * Gets the value at the given index.
   * @param index the index to retrieve the value.
   * @return the value at the index.
   */
  double getValueAsDouble(int index);
}

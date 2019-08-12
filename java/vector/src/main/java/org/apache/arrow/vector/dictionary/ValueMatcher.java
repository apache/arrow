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
import org.apache.arrow.vector.compare.RangeEqualsVisitor;

/**
 * A special {@link RangeEqualsVisitor} used to compare single values
 * which used in dictionary encoding.
 */
public class ValueMatcher {

  private RangeEqualsVisitor visitor;

  /**
   * Constructs a new instance.
   */
  public ValueMatcher(ValueVector dictionary) {
    this.visitor = new RangeEqualsVisitor(dictionary, 0, 0, 1);
    visitor.setNeedCheckType(false);
  }

  /**
   * Check if single value equals in the two vectors.
   * @param dictIndex index in dictionary
   * @param index index in the vector to encode.
   * @return true if value matches, otherwise false
   */
  public boolean match(int dictIndex, ValueVector vector, int index) {
    visitor.reset(index, dictIndex, 1);
    return vector.accept(visitor);
  }
}

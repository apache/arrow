/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.complex;

import org.apache.arrow.vector.UInt4Vector;

import com.google.common.base.Preconditions;

/**
 * A helper class that is used to track and populate empty values in repeated value vectors.
 */
public class EmptyValuePopulator {
  private final UInt4Vector offsets;

  public EmptyValuePopulator(UInt4Vector offsets) {
    this.offsets = Preconditions.checkNotNull(offsets, "offsets cannot be null");
  }

  /**
   * Marks all values since the last set as empty. The last set value is obtained from underlying offsets vector.
   *
   * @param lastIndex the last index (inclusive) in the offsets vector until which empty population takes place
   * @throws java.lang.IndexOutOfBoundsException if lastIndex is negative or greater than offsets capacity.
   */
  public void populate(int lastIndex) {
    if (lastIndex < 0) {
      throw new IndexOutOfBoundsException("index cannot be negative");
    }
    final UInt4Vector.Accessor accessor = offsets.getAccessor();
    final UInt4Vector.Mutator mutator = offsets.getMutator();
    final int lastSet = Math.max(accessor.getValueCount() - 1, 0);
    final int previousEnd = accessor.get(lastSet);//0 ? 0 : accessor.get(lastSet);
    for (int i = lastSet; i < lastIndex; i++) {
      mutator.setSafe(i + 1, previousEnd);
    }
    mutator.setValueCount(lastIndex + 1);
  }

}

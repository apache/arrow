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

import org.apache.arrow.vector.complex.RepeatedFixedWidthVectorLike;
import org.apache.arrow.vector.complex.RepeatedVariableWidthVectorLike;

/** Helper utility methods for allocating storage for Vectors. */
public class AllocationHelper {
  private AllocationHelper() {}

  /**
   * Allocates the vector.
   *
   * @param v The vector to allocate.
   * @param valueCount Number of values to allocate.
   * @param bytesPerValue bytes per value.
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the memory.
   */
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue) {
    allocate(v, valueCount, bytesPerValue, 5);
  }

  /**
   * Allocates memory for a vector assuming given number of values and their width.
   *
   * @param v The vector the allocate.
   * @param valueCount The number of elements to allocate.
   * @param bytesPerValue The bytes per value to use for allocating underlying storage
   * @param childValCount  If <code>v</code> is a repeated vector, this is number of child elements to allocate.
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the memory.
   */
  public static void allocatePrecomputedChildCount(
      ValueVector v,
      int valueCount,
      int bytesPerValue,
      int childValCount) {
    if (v instanceof FixedWidthVector) {
      ((FixedWidthVector) v).allocateNew(valueCount);
    } else if (v instanceof VariableWidthVector) {
      ((VariableWidthVector) v).allocateNew(valueCount * bytesPerValue, valueCount);
    } else if (v instanceof RepeatedFixedWidthVectorLike) {
      ((RepeatedFixedWidthVectorLike) v).allocateNew(valueCount, childValCount);
    } else if (v instanceof RepeatedVariableWidthVectorLike) {
      ((RepeatedVariableWidthVectorLike) v).allocateNew(childValCount * bytesPerValue, valueCount, childValCount);
    } else {
      v.allocateNew();
    }
  }

  /**
   * Allocates memory for a vector assuming given number of values and their width.
   *
   * @param v The vector the allocate.
   * @param valueCount The number of elements to allocate.
   * @param bytesPerValue The bytes per value to use for allocating underlying storage
   * @param repeatedPerTop  If <code>v</code> is a repeated vector, this is assumed number of elements per child.
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the memory
   */
  public static void allocate(ValueVector v, int valueCount, int bytesPerValue, int repeatedPerTop) {
    allocatePrecomputedChildCount(v, valueCount, bytesPerValue, repeatedPerTop * valueCount);
  }

  /**
   * Allocates the exact amount if v is fixed width, otherwise falls back to dynamic allocation
   *
   * @param v          value vector we are trying to allocate
   * @param valueCount size we are trying to allocate
   * @throws org.apache.arrow.memory.OutOfMemoryException if it can't allocate the memory
   */
  public static void allocateNew(ValueVector v, int valueCount) {
    if (v instanceof FixedWidthVector) {
      ((FixedWidthVector) v).allocateNew(valueCount);
    } else if (v instanceof VariableWidthVector) {
      ((VariableWidthVector) v).allocateNew(valueCount);
    } else {
      v.allocateNew();
    }
  }
}

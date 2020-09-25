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

package org.apache.arrow.tensor;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/** A tensor of doubles. */
public class Float8Tensor extends BaseTensor {
  public static final byte TYPE_WIDTH = 8;
  public static final ArrowType TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

  public Float8Tensor(BufferAllocator allocator, long[] shape, String[] names, long[] strides) {
    super(allocator, TYPE, TYPE_WIDTH, shape, names, strides);
  }

  @Override
  Object getObject(long[] indices) {
    return get(indices);
  }

  double get(long[] indices) {
    checkClosed();
    return data.getDouble(getElementIndex(indices));
  }

  void set(long[] indices, double value) {
    checkClosed();
    data.setDouble(getElementIndex(indices), value);
  }

  void copyFrom(double[] source) {
    long expectedElements = getElementCount();
    Preconditions.checkArgument(expectedElements == source.length,
        "Cannot copy tensor of %s elements to array of %s elements", expectedElements, source.length);
    int offset = 0;
    for (double value : source) {
      data.setDouble(offset, value);
      offset += typeWidth;
    }
  }

  void copyTo(double[] target) {
    long expectedElements = getElementCount();
    Preconditions.checkArgument(expectedElements == target.length,
        "Cannot copy tensor of %s elements to array of %s elements", expectedElements, target.length);
    int offset = 0;
    for (int i = 0; i < expectedElements; i++) {
      target[i] = data.getDouble(offset);
      offset += typeWidth;
    }
  }
}

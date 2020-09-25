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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFloatTensor {
  private static final double EPSILON = 1e-6;
  private BufferAllocator allocator;

  @Before
  public void setUp() {
    allocator = new RootAllocator(Integer.MAX_VALUE);
  }

  @After
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testConstructorValidation() {
    IllegalArgumentException err;
    err = assertThrows(IllegalArgumentException.class,
        () -> new Float8Tensor(allocator, new long[]{1}, null, new long[]{}));
    assertTrue(err.toString(), err.getMessage().contains("Dimensions (1) must match strides (0)"));

    err = assertThrows(IllegalArgumentException.class,
        () -> new Float8Tensor(allocator, new long[]{1}, new String[]{}, new long[]{8}));
    assertTrue(err.toString(), err.getMessage().contains("Dimensions must match names"));

    err = assertThrows(IllegalArgumentException.class,
        () -> new Float8Tensor(allocator, new long[]{1}, new String[]{"foo"}, new long[]{4}));
    assertTrue(err.toString(), err.getMessage().contains("0th stride 4 must be at least type width 8"));
  }

  @Test
  public void testGetters() {
    long[] shape = new long[]{2, 3, 4};
    String[] names = new String[]{"x", "y", "z"};
    long[] rowStrides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    long[] columnStrides = BaseTensor.columnMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    try (Float8Tensor tensor1 = new Float8Tensor(allocator, shape, names, rowStrides);
         Float8Tensor tensor2 = new Float8Tensor(allocator, shape, null, rowStrides);
         Float8Tensor tensor3 = new Float8Tensor(allocator, shape, null, columnStrides)) {
      assertEquals("Float8Tensor[2, 3, 4]", tensor1.toString());
      assertEquals(Float8Tensor.TYPE_WIDTH, tensor1.getTypeWidth());
      assertArrayEquals(shape, tensor1.getShape());
      assertArrayEquals(names, tensor1.getNames());
      assertArrayEquals(rowStrides, tensor1.getStrides());
      assertEquals(24, tensor1.getElementCount());
      assertTrue(tensor1.hasSameLayoutAs(tensor1));
      assertTrue(tensor1.hasSameLayoutAs(tensor2));
      assertTrue(tensor2.hasSameLayoutAs(tensor1));
      assertFalse(tensor1.hasSameLayoutAs(tensor3));

      assertTrue(tensor2.isRowMajor());
      assertFalse(tensor2.isColumnMajor());
      assertFalse(tensor3.isRowMajor());
      assertTrue(tensor3.isColumnMajor());
    }
  }

  @Test
  public void testGetSet() {
    long[] shape = new long[]{2, 3, 4};
    long[] strides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    try (Float8Tensor tensor = new Float8Tensor(allocator, shape, null, strides)) {
      assertThrows(IllegalArgumentException.class, () -> tensor.get(new long[]{-1}));
      assertThrows(IllegalArgumentException.class, () -> tensor.get(new long[]{0, 0, 0, 0}));
      assertThrows(IndexOutOfBoundsException.class, () -> tensor.get(new long[]{-1, 0, 0}));
      assertThrows(IndexOutOfBoundsException.class, () -> tensor.get(new long[]{0, 5, 2}));

      assertEquals(64, tensor.getElementIndex(new long[]{0, 2, 0}));
      assertEquals(0.0, tensor.get(new long[]{0, 2, 0}), EPSILON);
      assertEquals(0.0, (Double) tensor.getObject(new long[]{0, 2, 0}), EPSILON);
      tensor.set(new long[]{0, 2, 0}, 1.0);
      assertEquals(1.0, tensor.get(new long[]{0, 2, 0}), EPSILON);
      assertEquals(1.0, tensor.getDataBuffer().getDouble(64), EPSILON);
    }
    Float8Tensor tensor = new Float8Tensor(allocator, shape, null, strides);
    tensor.close();
    assertThrows(IllegalStateException.class, () -> tensor.get(new long[0]));
  }

  @Test
  public void testZeroTensor() {
    long[] shape = new long[]{2, 3, 4};
    long[] strides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    try (Float8Tensor tensor = new Float8Tensor(allocator, shape, null, strides)) {
      assertEquals(64, tensor.getElementIndex(new long[]{0, 2, 0}));
      tensor.set(new long[]{0, 2, 0}, 1.0);
      assertEquals(1.0, tensor.get(new long[]{0, 2, 0}), EPSILON);
      assertEquals(1.0, tensor.getDataBuffer().getDouble(64), EPSILON);
      tensor.zeroTensor();
      assertEquals(0.0, tensor.get(new long[]{0, 2, 0}), EPSILON);
      assertEquals(0.0, tensor.getDataBuffer().getDouble(64), EPSILON);
    }
  }

  @Test
  public void testTransferTo() {
    long[] shape = new long[]{2, 3, 4};
    long[] rowStrides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    long[] colStrides = BaseTensor.columnMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    BufferAllocator allocator2 = allocator.newChildAllocator("child", 0, allocator.getLimit());
    try (Float8Tensor tensor1 = new Float8Tensor(allocator2, shape, null, rowStrides);
         Float8Tensor tensor2 = new Float8Tensor(allocator, shape, null, rowStrides);
         Float8Tensor tensor3 = new Float8Tensor(allocator, shape, null, colStrides)) {
      long[] indices = {0, 0, 0};
      tensor1.set(indices, 5.0);
      tensor1.transferTo(tensor2);
      allocator2.close();
      assertThrows(IllegalStateException.class, () -> tensor1.get(indices));
      assertEquals(5.0, tensor2.get(indices), 5.0);

      assertThrows(UnsupportedOperationException.class, () -> tensor2.transferTo(tensor3));
    }
  }

  @Test
  public void testCopyToFrom() {
    long[] shape = new long[]{2, 3};
    long[] rowStrides = BaseTensor.rowMajorStrides(Float8Tensor.TYPE_WIDTH, shape);
    double[] rowMajorSource = new double[] {1, 2, 3, 4, 5, 6};
    double[] target = new double[6];
    try (Float8Tensor tensor = new Float8Tensor(allocator, shape, null, rowStrides)) {
      tensor.copyFrom(rowMajorSource);
      assertEquals(1.0, tensor.get(new long[]{0, 0}), EPSILON);
      assertEquals(2.0, tensor.get(new long[]{0, 1}), EPSILON);
      assertEquals(3.0, tensor.get(new long[]{0, 2}), EPSILON);
      assertEquals(4.0, tensor.get(new long[]{1, 0}), EPSILON);
      assertEquals(5.0, tensor.get(new long[]{1, 1}), EPSILON);
      assertEquals(6.0, tensor.get(new long[]{1, 2}), EPSILON);
      tensor.copyTo(target);
      assertArrayEquals(rowMajorSource, target, EPSILON);
    }
  }
}

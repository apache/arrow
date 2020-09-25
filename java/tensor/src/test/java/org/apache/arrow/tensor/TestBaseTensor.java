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

import org.junit.Test;

public class TestBaseTensor {
  @Test
  public void rowMajorStrides() {
    assertArrayEquals(new long[]{4}, BaseTensor.rowMajorStrides((byte) 4, new long[]{2}));
    assertArrayEquals(new long[]{12, 4}, BaseTensor.rowMajorStrides((byte) 4, new long[]{2, 3}));
    assertArrayEquals(new long[]{60, 20, 4}, BaseTensor.rowMajorStrides((byte) 4, new long[]{2, 3, 5}));
  }

  @Test
  public void columnMajorStrides() {
    assertArrayEquals(new long[]{4}, BaseTensor.columnMajorStrides((byte) 4, new long[]{2}));
    assertArrayEquals(new long[]{4, 8}, BaseTensor.columnMajorStrides((byte) 4, new long[]{2, 3}));
    assertArrayEquals(new long[]{4, 8, 24}, BaseTensor.columnMajorStrides((byte) 4, new long[]{2, 3, 5}));
  }
}

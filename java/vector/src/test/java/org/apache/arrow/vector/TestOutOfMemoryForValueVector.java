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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests cases where we expect to receive {@link OutOfMemoryException}.
 */
public class TestOutOfMemoryForValueVector {

  private static final String EMPTY_SCHEMA_PATH = "";

  private BufferAllocator allocator;

  @BeforeEach
  public void init() {
    allocator = new RootAllocator(200); // Start with low memory limit
  }

  @Test
  public void variableWidthVectorAllocateNew() {
    assertThrows(OutOfMemoryException.class, () -> {
      try (VarCharVector vector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
        vector.allocateNew();
      }
    });
  }

  @Test
  public void variableWidthVectorAllocateNewCustom() {
    assertThrows(OutOfMemoryException.class, () -> {
      try (VarCharVector vector = new VarCharVector(EMPTY_SCHEMA_PATH, allocator)) {
        vector.allocateNew(2342, 234);
      }
    });
  }

  @Test
  public void fixedWidthVectorAllocateNew() {
    assertThrows(OutOfMemoryException.class, () -> {
      try (IntVector vector = new IntVector(EMPTY_SCHEMA_PATH, allocator)) {
        vector.allocateNew();
      }
    });
  }

  @Test
  public void fixedWidthVectorAllocateNewCustom() {
    assertThrows(OutOfMemoryException.class, () -> {
      try (IntVector vector = new IntVector(EMPTY_SCHEMA_PATH, allocator)) {
        vector.allocateNew(2342);
      }
    });
  }

  @AfterEach
  public void terminate() {
    allocator.close();
  }
}

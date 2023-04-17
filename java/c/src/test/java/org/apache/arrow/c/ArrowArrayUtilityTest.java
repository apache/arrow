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

package org.apache.arrow.c;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArrowArrayUtilityTest {
  BufferAllocator allocator;
  ArrowArray arrowArray;
  ReferenceCountedArrowArray dummyHandle;

  @BeforeEach
  void beforeEach() {
    allocator = new RootAllocator();
    arrowArray = ArrowArray.allocateNew(allocator);
    dummyHandle = new ReferenceCountedArrowArray(arrowArray);
  }

  @AfterEach
  void afterEach() {
    dummyHandle.release();
    allocator.close();
  }

  // ------------------------------------------------------------
  // BufferImportTypeVisitor

  @Test
  void getBufferPtr() throws Exception {
    // Note values are all dummy values here
    try (BufferImportTypeVisitor visitor =
        new BufferImportTypeVisitor(allocator, dummyHandle, new ArrowFieldNode(0, 0), new long[]{0})) {

      // Too few buffers
      assertThrows(IllegalStateException.class, () -> visitor.getBufferPtr(new ArrowType.Bool(), 1));

      // Null where one isn't expected
      assertThrows(IllegalStateException.class, () -> visitor.getBufferPtr(new ArrowType.Bool(), 0));
    }
  }

  @Test
  void cleanupAfterFailure() throws Exception {
    // Note values are all dummy values here
    long address = MemoryUtil.UNSAFE.allocateMemory(16);
    try (BufferImportTypeVisitor visitor =
             new BufferImportTypeVisitor(allocator, dummyHandle, new ArrowFieldNode(0, 0), new long[] {address})) {
      // This fails, but only after we've already imported a buffer.
      assertThrows(IllegalStateException.class, () -> visitor.visit(new ArrowType.Int(32, true)));
    } finally {
      MemoryUtil.UNSAFE.freeMemory(address);
    }
  }

  @Test
  void bufferAssociatedWithAllocator() throws Exception {
    // Note values are all dummy values here
    final long bufferSize = 16;
    final long fieldLength = bufferSize / IntVector.TYPE_WIDTH;
    long address = MemoryUtil.UNSAFE.allocateMemory(bufferSize);
    long baseline = allocator.getAllocatedMemory();
    ArrowFieldNode fieldNode = new ArrowFieldNode(fieldLength, 0);
    try (BufferImportTypeVisitor visitor =
             new BufferImportTypeVisitor(allocator, dummyHandle, fieldNode, new long[] {0, address})) {
      List<ArrowBuf> buffers = visitor.visit(new ArrowType.Int(32, true));
      assertThat(buffers).hasSize(2);
      assertThat(buffers.get(0)).isNull();
      assertThat(buffers.get(1))
          .isNotNull()
          .extracting(ArrowBuf::getReferenceManager)
          .extracting(ReferenceManager::getAllocator)
          .isEqualTo(allocator);
      assertThat(allocator.getAllocatedMemory()).isEqualTo(baseline + bufferSize);
    } finally {
      MemoryUtil.UNSAFE.freeMemory(address);
    }
    assertThat(allocator.getAllocatedMemory()).isEqualTo(baseline);
  }

  // ------------------------------------------------------------
  // ReferenceCountedArrowArray

  @Test
  void releaseRetain() {
    ArrowArray array = ArrowArray.allocateNew(allocator);
    ReferenceCountedArrowArray handle = new ReferenceCountedArrowArray(array);
    assertThat(array.isClosed()).isFalse();
    handle.retain();
    assertThat(array.isClosed()).isFalse();
    handle.release();
    assertThat(array.isClosed()).isFalse();
    handle.release();
    assertThat(array.isClosed()).isTrue();

    assertThrows(IllegalStateException.class, handle::release);
    assertThrows(IllegalStateException.class, handle::retain);
  }

  @Test
  void associate() {
    final long bufferSize = 16;
    final long address = MemoryUtil.UNSAFE.allocateMemory(bufferSize);
    try {
      ArrowArray array = ArrowArray.allocateNew(allocator);
      ReferenceCountedArrowArray handle = new ReferenceCountedArrowArray(array);
      assertThat(array.isClosed()).isFalse();
      ArrowBuf buf = handle.unsafeAssociateAllocation(allocator, bufferSize, address);
      assertThat(array.isClosed()).isFalse();
      buf.close();
      assertThat(array.isClosed()).isFalse();
      handle.release();
      assertThat(array.isClosed()).isTrue();
    } finally {
      MemoryUtil.UNSAFE.freeMemory(address);
    }
  }
}

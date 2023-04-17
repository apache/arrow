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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ForeignAllocation;

/**
 * The owner of an imported C Data Interface array.
 *
 * <p>There is a fundamental mismatch here between memory allocation schemes: AllocationManager represents a single
 * allocation (= a single address and length). But an ArrowArray combines multiple allocations behind a single
 * deallocation callback. This class bridges the two by tracking a reference count, so that the single callback
 * can be managed by multiple {@link ForeignAllocation} instances.
 */
final class ReferenceCountedArrowArray {
  private final ArrowArray array;
  private final AtomicInteger refCnt;

  ReferenceCountedArrowArray(ArrowArray array) {
    this.array = array;
    this.refCnt = new AtomicInteger(1);
  }

  void retain() {
    if (refCnt.addAndGet(1) - 1 <= 0) {
      throw new IllegalStateException("Tried to retain a released ArrowArray");
    }
  }

  void release() {
    int refcnt = refCnt.addAndGet(-1);
    if (refcnt == 0) {
      array.release();
      array.close();
    } else if (refcnt < 0) {
      throw new IllegalStateException("Reference count went negative for imported ArrowArray");
    }
  }

  /**
   * Create an ArrowBuf wrapping a buffer from this ArrowArray associated with the given BufferAllocator.
   *
   * <p>This method is "unsafe" because there is no validation of the given capacity or address. If the returned
   * buffer is not freed, a memory leak will occur.
   */
  ArrowBuf unsafeAssociateAllocation(BufferAllocator trackingAllocator, long capacity, long memoryAddress) {
    retain();
    return trackingAllocator.wrapForeignAllocation(new ForeignAllocation(capacity, memoryAddress) {
      @Override
      protected void release0() {
        ReferenceCountedArrowArray.this.release();
      }
    });
  }
}

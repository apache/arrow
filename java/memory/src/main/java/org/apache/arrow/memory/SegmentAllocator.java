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

package org.apache.arrow.memory;

import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.OutOfDirectMemoryError;

/**
 * A segment allocator allocates a buffer whose size is a multiple of the segment size.
 */
public class SegmentAllocator extends BaseAllocator {

  protected final int segmentSize;

  public SegmentAllocator(final long limit, final int segmentSize) {
    this(AllocationListener.NOOP, limit, segmentSize);
  }

  public SegmentAllocator(final AllocationListener listener, final long limit, final int segmentSize) {
    this(null, listener, "Segment Allocator", 0, limit, segmentSize);
  }

  /**
   * Initialize an allocator
   * @param parentAllocator   parent allocator. null if defining a root allocator
   * @param listener          listener callback. Must be non-null -- use
   *                          {@link AllocationListener#NOOP} if no listener desired
   * @param name              name of this allocator
   * @param initReservation   initial reservation. Cannot be modified after construction
   * @param maxAllocation     limit. Allocations past the limit fail. Can be modified after construction
   * @param segmentSize segment size. Must be greater than or equal to 1024, and must be a power of 2.
   */
  public SegmentAllocator(
          final BaseAllocator parentAllocator,
          final AllocationListener listener,
          final String name,
          final long initReservation,
          final long maxAllocation,
          final int segmentSize) {
    super(parentAllocator, listener, name, initReservation, maxAllocation);

    if (segmentSize < 1024L) {
      throw new IllegalArgumentException("The segment size cannot be smaller than 1024");
    }
    if ((segmentSize & (segmentSize - 1)) != 0) {
      throw new IllegalArgumentException("The segment size must be a power of 2");
    }
    this.segmentSize = segmentSize;
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize, BufferManager manager) {
    assertOpen();

    Preconditions.checkArgument(initialRequestSize >= 0, "the requested size must be non-negative");

    if (initialRequestSize == 0) {
      return empty;
    }

    // round to next largest multiple of the segment size
    final int actualRequestSize =
            initialRequestSize / segmentSize * segmentSize + (initialRequestSize % segmentSize == 0 ? 0 : segmentSize);

    listener.onPreAllocation(actualRequestSize);

    AllocationOutcome outcome = this.allocateBytes(actualRequestSize);
    if (!outcome.isOk()) {
      if (listener.onFailedAllocation(actualRequestSize, outcome)) {
        // Second try, in case the listener can do something about it
        outcome = this.allocateBytes(actualRequestSize);
      }
      if (!outcome.isOk()) {
        throw new OutOfMemoryException(createErrorMsg(this, actualRequestSize, initialRequestSize));
      }
    }

    boolean success = false;
    try {
      ArrowBuf buffer = bufferWithoutReservation(actualRequestSize, manager);
      success = true;
      listener.onAllocation(actualRequestSize);
      return buffer;
    } catch (OutOfMemoryError e) {
      /*
       * OutOfDirectMemoryError is thrown by Netty when we exceed the direct memory limit defined by
       * -XX:MaxDirectMemorySize. OutOfMemoryError with "Direct buffer memory" message is thrown by
       * java.nio.Bits when we exceed the direct memory limit. This should never be hit in practice
       * as Netty is expected to throw an OutOfDirectMemoryError first.
       */
      if (e instanceof OutOfDirectMemoryError || "Direct buffer memory".equals(e.getMessage())) {
        throw new OutOfMemoryException(e);
      }
      throw e;
    } finally {
      if (!success) {
        releaseBytes(actualRequestSize);
      }
    }

  }

  @Override
  public BufferAllocator newChildAllocator(
          final String name,
          final AllocationListener listener,
          final long initReservation,
          final long maxAllocation) {
    assertOpen();

    final SegmentAllocator childAllocator =
            new SegmentAllocator(this, listener, name, initReservation, maxAllocation, this.segmentSize);

    if (DEBUG) {
      synchronized (DEBUG_LOCK) {
        childAllocators.put(childAllocator, childAllocator);
        historicalLog.recordEvent("allocator[%s] created new child allocator[%s]", name,
                childAllocator.name);
      }
    }
    this.listener.onChildAdded(this, childAllocator);

    return childAllocator;
  }

  public int getSegmentSize() {
    return segmentSize;
  }
}

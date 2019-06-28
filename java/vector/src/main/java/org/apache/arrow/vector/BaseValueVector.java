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

import java.util.Collections;
import java.util.Iterator;

import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.DataSizeRoundingUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.util.TransferPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

/**
 * Base class for other Arrow Vector Types.  Provides basic functionality around
 * memory management.
 */
public abstract class BaseValueVector implements ValueVector {
  private static final Logger logger = LoggerFactory.getLogger(BaseValueVector.class);

  public static final String MAX_ALLOCATION_SIZE_PROPERTY = "arrow.vector.max_allocation_bytes";
  public static final int MAX_ALLOCATION_SIZE = Integer.getInteger(MAX_ALLOCATION_SIZE_PROPERTY, Integer.MAX_VALUE);
  /*
   * For all fixed width vectors, the value and validity buffers are sliced from a single buffer.
   * Similarly, for variable width vectors, the offsets and validity buffers are sliced from a
   * single buffer. To ensure the single buffer is power-of-2 size, the initial value allocation
   * should be less than power-of-2. For IntVectors, this comes to 3970*4 (15880) for the data
   * buffer and 504 bytes for the validity buffer, totalling to 16384 (2^16).
   */
  public static final int INITIAL_VALUE_ALLOCATION = 3970;

  protected final BufferAllocator allocator;
  protected final String name;

  protected BaseValueVector(String name, BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.name = name;
  }

  @Override
  public String toString() {
    return super.toString() + "[name = " + name + ", ...]";
  }

  @Override
  public void clear() {
  }

  @Override
  public void close() {
    clear();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator allocator) {
    return getTransferPair(name, allocator);
  }

  @Override
  public Iterator<ValueVector> iterator() {
    return Collections.emptyIterator();
  }

  /**
   * Checks to ensure that every buffer <code>vv</code> uses
   * has a positive reference count, throws if this precondition
   * isn't met.  Returns true otherwise.
   */
  public static boolean checkBufRefs(final ValueVector vv) {
    for (final ArrowBuf buffer : vv.getBuffers(false)) {
      if (buffer.refCnt() <= 0) {
        throw new IllegalStateException("zero refcount");
      }
    }

    return true;
  }

  @Override
  public BufferAllocator getAllocator() {
    return allocator;
  }

  void compareTypes(BaseValueVector target, String caller) {
    if (this.getMinorType() != target.getMinorType()) {
      throw new UnsupportedOperationException(caller + " should have vectors of exact same type");
    }
  }

  protected ArrowBuf releaseBuffer(ArrowBuf buffer) {
    buffer.getReferenceManager().release();
    buffer = allocator.getEmpty();
    return buffer;
  }

  /* number of bytes for the validity buffer for the given valueCount */
  protected static int getValidityBufferSizeFromCount(final int valueCount) {
    return DataSizeRoundingUtil.divideBy8Ceil(valueCount);
  }

  /* round up bytes for the validity buffer for the given valueCount */
  private static long roundUp8ForValidityBuffer(int valueCount) {
    return ((valueCount + 63) >> 6) << 3;
  }

  long computeCombinedBufferSize(int valueCount, int typeWidth) {
    Preconditions.checkArgument(valueCount >= 0, "valueCount must be >= 0");
    Preconditions.checkArgument(typeWidth >= 0, "typeWidth must be >= 0");

    // compute size of validity buffer.
    long bufferSize = roundUp8ForValidityBuffer(valueCount);

    // add the size of the value buffer.
    if (typeWidth == 0) {
      // for boolean type, value-buffer and validity-buffer are of same size.
      bufferSize *= 2;
    } else {
      bufferSize += DataSizeRoundingUtil.roundUpTo8Multiple(valueCount * typeWidth);
    }
    return BaseAllocator.nextPowerOfTwo(bufferSize);
  }

  /**
   * Container for primitive vectors (1 for the validity bit-mask and one to hold the values).
   */
  class DataAndValidityBuffers {
    private ArrowBuf dataBuf;
    private ArrowBuf validityBuf;

    DataAndValidityBuffers(ArrowBuf dataBuf, ArrowBuf validityBuf) {
      this.dataBuf = dataBuf;
      this.validityBuf = validityBuf;
    }

    ArrowBuf getDataBuf() {
      return dataBuf;
    }

    ArrowBuf getValidityBuf() {
      return validityBuf;
    }
  }

  DataAndValidityBuffers allocFixedDataAndValidityBufs(int valueCount, int typeWidth) {
    long bufferSize = computeCombinedBufferSize(valueCount, typeWidth);
    assert bufferSize <= MAX_ALLOCATION_SIZE;

    int validityBufferSize;
    int dataBufferSize;
    if (typeWidth == 0) {
      validityBufferSize = dataBufferSize = (int) (bufferSize / 2);
    } else {
      // Due to roundup to power-of-2 allocation, the bufferSize could be greater than the
      // requested size. Utilize the allocated buffer fully.;
      int actualCount = (int) ((bufferSize * 8.0) / (8 * typeWidth + 1));
      do {
        validityBufferSize = (int) roundUp8ForValidityBuffer(actualCount);
        dataBufferSize = DataSizeRoundingUtil.roundUpTo8Multiple(actualCount * typeWidth);
        if (validityBufferSize + dataBufferSize <= bufferSize) {
          break;
        }
        --actualCount;
      } while (true);
    }


    /* allocate combined buffer */
    ArrowBuf combinedBuffer = allocator.buffer((int) bufferSize);

    /* slice into requested lengths */
    ArrowBuf dataBuf = null;
    ArrowBuf validityBuf = null;
    int bufferOffset = 0;
    for (int numBuffers = 0; numBuffers < 2; ++numBuffers) {
      int len = (numBuffers == 0 ? dataBufferSize : validityBufferSize);
      ArrowBuf buf = combinedBuffer.slice(bufferOffset, len);
      buf.getReferenceManager().retain();
      buf.readerIndex(0);
      buf.writerIndex(0);

      bufferOffset += len;
      if (numBuffers == 0) {
        dataBuf = buf;
      } else {
        validityBuf = buf;
      }
    }
    combinedBuffer.getReferenceManager().release();
    return new DataAndValidityBuffers(dataBuf, validityBuf);
  }

  public static ArrowBuf transferBuffer(final ArrowBuf srcBuffer, final BufferAllocator targetAllocator) {
    final ReferenceManager referenceManager = srcBuffer.getReferenceManager();
    return referenceManager.transferOwnership(srcBuffer, targetAllocator).getTransferredBuffer();
  }
}


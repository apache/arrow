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

package io.netty.buffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.MathUtil;
import org.apache.arrow.memory.*;
import org.apache.arrow.memory.BaseAllocator.Verbosity;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;

import io.netty.util.internal.PlatformDependent;

/**
 * ArrowBuf serves as a facade over underlying memory by providing
 * several access APIs to read/write data into a chunk of direct
 * memory. All the accounting, ownership and reference management
 * is done by {@link ReferenceManager} and ArrowBuf can work
 * with a custom user provided implementation of ReferenceManager
 *
 * Two important instance variables of an ArrowBuf:
 *
 * (1) address - starting virtual address in the underlying memory
 * chunk that this ArrowBuf has access to
 *
 * (2) length - length (in bytes) in the underlying memory chunk
 *  that this ArrowBuf has access to
 *
 * The mangement (allocation, deallocation, reference counting etc) for
 * the memory chunk is not done by ArrowBuf.
 *
 * Default implementation of ReferenceManager, allocation is in
 * {@link BaseAllocator}, {@link BufferLedger} and {@link AllocationManager}
 */
public final class ArrowBuf implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowBuf.class);

  private static final AtomicLong idGenerator = new AtomicLong(0);
  private static final int LOG_BYTES_PER_ROW = 10;
  private final long id = idGenerator.incrementAndGet();
  private final ReferenceManager referenceManager;
  private final long addr;
  private final boolean isEmpty;
  private int readerIndex;
  private int writerIndex;
  private final HistoricalLog historicalLog = BaseAllocator.DEBUG ?
      new HistoricalLog(BaseAllocator.DEBUG_LOG_LENGTH, "ArrowBuf[%d]", id) : null;
  private volatile int length;

  /**
   * Constructs a new ArrowBuf
   * @param referenceManager The memory manager to track memory usage and reference count of this buffer
   * @param length The  byte length of this buffer
   * @param isEmpty  Indicates if this buffer is empty which enables some optimizations.
   */
  public ArrowBuf(
      final ReferenceManager referenceManager,
      final int length,
      final long memoryAddress,
      boolean isEmpty) {
    this.referenceManager = referenceManager;
    this.isEmpty = isEmpty;
    this.addr = memoryAddress;
    this.length = length;
    this.readerIndex = 0;
    this.writerIndex = 0;
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent("create()");
    }
  }

  public int refCnt() {
    return isEmpty ? 1 : referenceManager.getRefCount();
  }

  private void checkIndexD(int index, int fieldLength) {
    ensureAccessible();
    if (fieldLength < 0) {
      throw new IllegalArgumentException("length: " + fieldLength + " (expected: >= 0)");
    }
    if (index < 0 || index > capacity() - fieldLength) {
      if (BaseAllocator.DEBUG) {
        historicalLog.logHistory(logger);
      }
      throw new IndexOutOfBoundsException(String.format(
          "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
    }
  }

  /**
   * Allows a function to determine whether not reading a particular string of bytes is valid.
   *
   * <p>Will throw an exception if the memory is not readable for some reason. Only doesn't
   * something in the case that
   * AssertionUtil.BOUNDS_CHECKING_ENABLED is true.
   *
   * @param start The starting position of the bytes to be read.
   * @param end   The exclusive endpoint of the bytes to be read.
   */
  public void checkBytes(int start, int end) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(start, end - start);
    }
  }

  private void chk(int index, int width) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, width);
    }
  }

  private void ensure(int width) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      ensureWritable(width);
    }
  }

  public ReferenceManager getReferenceManager() {
    return referenceManager;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public int capacity() {
    return length;
  }

  public synchronized ArrowBuf capacity(int newCapacity) {

    if (newCapacity == length) {
      return this;
    }

    Preconditions.checkArgument(newCapacity >= 0);

    if (newCapacity < length) {
      length = newCapacity;
      return this;
    }

    throw new UnsupportedOperationException("Buffers don't support resizing that increases the size.");
  }

  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  public int readableBytes() {
    Preconditions.checkState(writerIndex >= readerIndex, "Writer index cannot be less than reader index");
    return writerIndex - readerIndex;
  }

  public ArrowBuf slice() {
    return slice(readerIndex, readableBytes());
  }

  public ArrowBuf slice(int index, int length) {
    if (isEmpty) {
      return this;
    }

    Preconditions.checkPositionIndex(index, this.length);
    Preconditions.checkPositionIndex(index + length, this.length);

    /*
     * Re the behavior of reference counting, see http://netty.io/wiki/reference-counted-objects
     * .html#wiki-h3-5, which
     * explains that derived buffers share their reference count with their parent
     */
    final ArrowBuf newBuf = referenceManager.deriveBuffer(this, index, length);
    newBuf.writerIndex(length);
    return newBuf;
  }

  public ByteBuffer nioBuffer() {
    // TODO SIDD
    return null;
  }

  public ByteBuffer nioBuffer(int index, int length) {
    // TODO SIDD
    return null;
  }

  public long memoryAddress() {
    return this.addr;
  }

  @Override
  public String toString() {
    return String.format("ArrowBuf[%d], address:%d, length:%d", id, memoryAddress(), length);
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    // identity equals only.
    return this == obj;
  }

  /**
   * IMPORTANT NOTE
   * The data getters and setters work with a caller provided
   * index. This index is 0 based and since ArrowBuf has access
   * to a portion of underlying chunk of memory starting at
   * some address, we convert the given relative index into
   * absolute index as memory address + index.
   *
   * Example:
   *
   * Let's say we have an underlying chunk of memory of length 64 bytes
   * Now let's say we have an ArrowBuf that has access to the chunk
   * from offset 4 for length of 16 bytes.
   *
   * If the starting virtual address of chunk is MAR, then memory
   * address of this ArrowBuf is MAR + offset -- this is what is stored
   * in variable addr. See the BufferLedger and AllocationManager code
   * for the implementation of ReferenceManager that manages a
   * chunk of memory and creates ArrowBuf with access to a range of
   * bytes within the chunk (or the entire chunk)
   *
   * So now to get/set data, we will do => addr + index
   * This logic is put in method addr(index) and is frequently
   * used in get/set data methods to compute the absolute
   * byte address for get/set operation in the underlying chunk
   *
   * @param index the index at which we the user wants to read/write
   * @return the absolute address within the memro
   */
  private long addr(int index) {
    return addr + index;
  }

  public long getLong(int index) {
    chk(index, 8);
    return PlatformDependent.getLong(addr(index));
  }

  public ArrowBuf setLong(int index, long value) {
    chk(index, 8);
    PlatformDependent.putLong(addr(index), value);
    return this;
  }

  public float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  public ArrowBuf setFloat(int index, float value) {
    chk(index, 4);
    PlatformDependent.putInt(addr(index), Float.floatToRawIntBits(value));
    return this;
  }

  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  public ArrowBuf setDouble(int index, double value) {
    chk(index, 8);
    PlatformDependent.putLong(addr(index), Double.doubleToRawLongBits(value));
    return this;
  }

  public char getChar(int index) {
    return (char) getShort(index);
  }

  public ArrowBuf setChar(int index, int value) {
    chk(index, 2);
    PlatformDependent.putShort(addr(index), (short) value);
    return this;
  }

  public int getInt(int index) {
    chk(index, 4);
    return PlatformDependent.getInt(addr(index));
  }

  public ArrowBuf setInt(int index, int value) {
    chk(index, 4);
    PlatformDependent.putInt(addr(index), value);
    return this;
  }

  public short getShort(int index) {
    chk(index, 2);
    return PlatformDependent.getShort(addr(index));
  }

  public ArrowBuf setShort(int index, int value) {
    chk(index, 2);
    PlatformDependent.putShort(addr(index), (short) value);
    return this;
  }

  public ArrowBuf writeShort(int value) {
    ensure(2);
    PlatformDependent.putShort(addr(writerIndex), (short) value);
    writerIndex += 2;
    return this;
  }

  public ArrowBuf writeInt(int value) {
    ensure(4);
    PlatformDependent.putInt(addr(writerIndex), value);
    writerIndex += 4;
    return this;
  }

  public ArrowBuf writeLong(long value) {
    ensure(8);
    PlatformDependent.putLong(addr(writerIndex), value);
    writerIndex += 8;
    return this;
  }

  public ArrowBuf writeFloat(float value) {
    ensure(4);
    PlatformDependent.putInt(addr(writerIndex), Float.floatToRawIntBits(value));
    writerIndex += 4;
    return this;
  }

  public ArrowBuf writeDouble(double value) {
    ensure(8);
    PlatformDependent.putLong(addr(writerIndex), Double.doubleToRawLongBits(value));
    writerIndex += 8;
    return this;
  }

  public ArrowBuf setByte(int index, int value) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), (byte) value);
    return this;
  }

  public ArrowBuf setByte(int index, byte b) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), b);
    return this;
  }

  public byte getByte(int index) {
    chk(index, 1);
    return PlatformDependent.getByte(addr(index));
  }

  // TODO: do bound checking properly for the following APIs

  public ArrowBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    final int end = dstIndex + length - 1;
    for (int i = dstIndex; i <= end; i++) {
      dst[i] = PlatformDependent.getByte(addr(index));
    }
    return this;
  }

  public ArrowBuf getBytes(int index, ByteBuffer dst) {
    int toRead = length;
    long address = addr(index);
    // word by word memcpy
    while (toRead >= 8) {
      dst.putLong(PlatformDependent.getLong(address));
      address += 8;
      toRead -= 8;
    }
    while (toRead > 0) {
      dst.put(PlatformDependent.getByte(address));
      --toRead;
    }
    return this;
  }

  public ArrowBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    if (length != 0) {
      // memcpy from source byte array at the starting index (srcIndex)
      // to the underlying chunk of memory this ArrowBuf has access to
      // at the given index (index)
      PlatformDependent.copyMemory(src, srcIndex, addr(index), (long)length);
    }
    return this;
  }

  public ArrowBuf setBytes(int index, ByteBuffer src) {
    int length = src.remaining();
    long dstAddress = addr(index);
    if(length != 0) {
      if(src.isDirect()) {
        final long srcAddress = PlatformDependent.directBufferAddress(src) + (long)src.position();
        PlatformDependent.copyMemory(srcAddress, dstAddress, (long)src.remaining());
        src.position(src.position() + length);
      } else if(src.hasArray()) {
        PlatformDependent.copyMemory(src.array(), src.arrayOffset() + src.position(), dstAddress, (long)length);
        src.position(src.position() + length);
      } else {
        while (length >= 8) {
          PlatformDependent.putLong(dstAddress, src.getLong());
          length -= 8;
          dstAddress += 8;
        }
        while (length > 0) {
          PlatformDependent.putByte(dstAddress, src.get());
        }
      }
    }
    return this;
  }

  /**
   * Copies length bytes from src starting at srcIndex
   * to this buffer starting at index.
   */
  public ArrowBuf setBytes(int index, ByteBuffer src, int srcIndex, int length) {
    final long dstAddress = addr(index);
    if (src.isDirect()) {
      final long srcAddress = PlatformDependent.directBufferAddress(src) + srcIndex;
      PlatformDependent.copyMemory(srcAddress, dstAddress, length);
    } else {
      if (srcIndex == 0 && src.capacity() == length) {
        setBytes(index, src);
      } else {
        ByteBuffer newBuf = src.duplicate();
        newBuf.position(srcIndex);
        newBuf.limit(srcIndex + length);
        setBytes(index, newBuf);
      }
    }

    return this;
  }

  @Override
  public void close() {
    referenceManager.release();
  }

  /**
   * Returns the possible memory consumed by this ArrowBuf in the worse case scenario.
   * (not shared, connected to larger underlying buffer of allocated memory)
   *
   * @return Size in bytes.
   */
  public int getPossibleMemoryConsumed() {
    return isEmpty ? 0 : referenceManager.getSize();
  }

  /**
   * Return that is Accounted for by this buffer (and its potentially shared siblings within the
   * context of the associated allocator).
   * TODO SIDD: I believe the following method should not be in ArrowBuf
   *
   * @return Size in bytes.
   */
  public int getActualMemoryConsumed() {
    return isEmpty ? 0 : referenceManager.getAccountedSize();
  }

  /**
   * Return the buffer's byte contents in the form of a hex dump.
   *
   * @param start  the starting byte index
   * @param length how many bytes to log
   * @return A hex dump in a String.
   */
  public String toHexString(final int start, final int length) {
    final int roundedStart = (start / LOG_BYTES_PER_ROW) * LOG_BYTES_PER_ROW;

    final StringBuilder sb = new StringBuilder("buffer byte dump\n");
    int index = roundedStart;
    for (int nLogged = 0; nLogged < length; nLogged += LOG_BYTES_PER_ROW) {
      sb.append(String.format(" [%05d-%05d]", index, index + LOG_BYTES_PER_ROW - 1));
      for (int i = 0; i < LOG_BYTES_PER_ROW; ++i) {
        try {
          final byte b = getByte(index++);
          sb.append(String.format(" 0x%02x", b));
        } catch (IndexOutOfBoundsException ioob) {
          sb.append(" <ioob>");
        }
      }
      sb.append('\n');
    }
    return sb.toString();
  }

  /**
   * Get the integer id assigned to this ArrowBuf for debugging purposes.
   *
   * @return integer id
   */
  public long getId() {
    return id;
  }

  /** Returns all ledger information with stack traces as a string. */
  public String toVerboseString() {
    if (isEmpty) {
      return toString();
    }

    StringBuilder sb = new StringBuilder();
    // TODO SIDD
    //referenceManager.print(sb, 0, Verbosity.LOG_WITH_STACKTRACE);
    return sb.toString();
  }

  /**
   * Prints information of this buffer into <code>sb</code> at the given
   * indentation and verbosity level.
   *
   * <p>It will include history if BaseAllocator.DEBUG is true and
   * the verbosity.includeHistoricalLog are true.
   *
   */
  public void print(StringBuilder sb, int indent, Verbosity verbosity) {
    BaseAllocator.indent(sb, indent).append(toString());

    if (BaseAllocator.DEBUG && !isEmpty && verbosity.includeHistoricalLog) {
      sb.append("\n");
      historicalLog.buildHistory(sb, indent + 1, verbosity.includeStackTraces);
    }
  }

  public int readerIndex() {
    return readerIndex;
  }

  public int writerIndex() {
    return writerIndex;
  }

  public ArrowBuf readerIndex(int readerIndex) {
    this.readerIndex = readerIndex;
    return this;
  }

  public ArrowBuf writerIndex(int writerIndex) {
    this.writerIndex = writerIndex;
    return this;
  }

  private void ensureAccessible() {
    if(this.refCnt() == 0) {
      throw new IllegalReferenceCountException(0);
    }
  }

  private void checkIndex(int index, int fieldLength) {
    this.ensureAccessible();
    this.checkIndex0(index, fieldLength);
  }

  private void  checkIndex0(int index, int fieldLength) {
    if(MathUtil.isOutOfBounds(index, fieldLength, this.capacity())) {
      throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))",
        new Object[]{Integer.valueOf(index), Integer.valueOf(fieldLength), Integer.valueOf(this.capacity())}));
    }
  }

  public ArrowBuf setZero(int index, int length) {
    if(length == 0) {
      return this;
    } else {
      this.checkIndex(index, length);
      int nLong = length >>> 3;
      int nBytes = length & 7;

      int i;
      for(i = nLong; i > 0; --i) {
        setLong(index, 0L);
        index += 8;
      }

      if(nBytes == 4) {
        setInt(index, 0);
      } else if(nBytes < 4) {
        for(i = nBytes; i > 0; --i) {
          setByte(index, 0);
          ++index;
        }
      } else {
        setInt(index, 0);
        index += 4;

        for(i = nBytes - 4; i > 0; --i) {
          setByte(index, 0);
          ++index;
        }
      }

      return this;
    }
  }
}

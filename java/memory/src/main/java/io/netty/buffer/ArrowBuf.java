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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BaseAllocator.Verbosity;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;

import io.netty.util.IllegalReferenceCountException;
import io.netty.util.internal.MathUtil;
import io.netty.util.internal.PlatformDependent;

/**
 * ArrowBuf serves as a facade over underlying memory by providing
 * several access APIs to read/write data into a chunk of direct
 * memory. All the accounting, ownership and reference management
 * is done by {@link ReferenceManager} and ArrowBuf can work
 * with a custom user provided implementation of ReferenceManager
 * <p>
 * Two important instance variables of an ArrowBuf:
 * (1) address - starting virtual address in the underlying memory
 * chunk that this ArrowBuf has access to
 * (2) length - length (in bytes) in the underlying memory chunk
 * that this ArrowBuf has access to
 * </p>
 * <p>
 * The mangement (allocation, deallocation, reference counting etc) for
 * the memory chunk is not done by ArrowBuf.
 * Default implementation of ReferenceManager, allocation is in
 * {@link BaseAllocator}, {@link BufferLedger} and {@link AllocationManager}
 * </p>
 */
public final class ArrowBuf implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ArrowBuf.class);

  private static final int SHORT_SIZE = Short.BYTES;
  private static final int INT_SIZE = Integer.BYTES;
  private static final int FLOAT_SIZE = Float.BYTES;
  private static final int DOUBLE_SIZE = Double.BYTES;
  private static final int LONG_SIZE = Long.BYTES;

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

  /**
   * For get/set operations, reference count should be >= 1.
   */
  private void ensureAccessible() {
    if (this.refCnt() == 0) {
      throw new IllegalReferenceCountException(0);
    }
  }

  public NettyArrowBuf asNettyBuffer() {
    return new NettyArrowBuf((UnsafeDirectLittleEndian)referenceManager.getUnderlying(),
      this,
      referenceManager.getByteBufAllocator(),
      referenceManager.getOffsetForBuffer(this),
      length);
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

  public int writableBytes() {
    return capacity() - writerIndex;
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

  /*
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

  /*-------------------------------------------------*
   | Following are a set of fast path data set and   |
   | get APIs to write/read data from ArrowBuf       |
   | at a given index (0 based relative to this      |
   | ArrowBuf and not relative to the underlying     |
   | memory chunk).                                  |
   |                                                 |
   *-------------------------------------------------*/

  /**
   * Helper function to do bounds checking at a particular
   * index for particular length of data.
   * @param index index (0 based relative to this ArrowBuf)
   * @param length provided length of data for get/set
   */
  private void chk(int index, int length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, length);
    }
  }

  private void checkIndexD(int index, int fieldLength) {
    // check reference count
    ensureAccessible();
    // check bounds
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

  public long getLong(int index) {
    chk(index, LONG_SIZE);
    return PlatformDependent.getLong(addr(index));
  }

  public void setLong(int index, long value) {
    chk(index, LONG_SIZE);
    PlatformDependent.putLong(addr(index), value);
  }

  public float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  public void setFloat(int index, float value) {
    chk(index, FLOAT_SIZE);
    PlatformDependent.putInt(addr(index), Float.floatToRawIntBits(value));
  }

  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  public void setDouble(int index, double value) {
    chk(index, DOUBLE_SIZE);
    PlatformDependent.putLong(addr(index), Double.doubleToRawLongBits(value));
  }

  public char getChar(int index) {
    return (char) getShort(index);
  }

  public void setChar(int index, int value) {
    chk(index, SHORT_SIZE);
    PlatformDependent.putShort(addr(index), (short) value);
  }

  public int getInt(int index) {
    chk(index, INT_SIZE);
    return PlatformDependent.getInt(addr(index));
  }

  public void setInt(int index, int value) {
    chk(index, INT_SIZE);
    PlatformDependent.putInt(addr(index), value);
  }

  public void setShort(int index, int value) {
    chk(index, SHORT_SIZE);
    PlatformDependent.putShort(addr(index), (short) value);
  }

  public short getShort(int index) {
    chk(index, INT_SIZE);
    return PlatformDependent.getShort(addr(index));
  }

  public void setByte(int index, int value) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), (byte) value);
  }

  public void setByte(int index, byte b) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), b);
  }

  public byte getByte(int index) {
    chk(index, 1);
    return PlatformDependent.getByte(addr(index));
  }

  /*--------------------------------------------------*
   | Following are another set of data set APIs       |
   | that directly work with writerIndex              |
   |                                                  |
   *--------------------------------------------------*/

  /**
   * Helper function to do bound checking w.r.t writerIndex
   * by checking if we can set "length" bytes of data at the
   * writerIndex in this ArrowBuf.
   * @param length provided length of data for set
   */
  private void ensureWritable(final int length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      Preconditions.checkArgument(length >= 0, "expecting non-negative length");
      // check reference count
      this.ensureAccessible();
      // check bounds
      if (length > writableBytes()) {
        throw new IndexOutOfBoundsException(
          String.format("writerIndex(%d) + length(%d) exceeds capacity(%d)", writerIndex, length, capacity()));
      }
    }
  }

  /**
   * Helper function to do bound checking w.r.t readerIndex
   * by checking if we can read "length" bytes of data at the
   * readerIndex in this ArrowBuf.
   * @param length provided length of data for get
   */
  private void ensureReadable(final int length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      Preconditions.checkArgument(length >= 0, "expecting non-negative length");
      // check reference count
      this.ensureAccessible();
      // check bounds
      if (length > readableBytes()) {
        throw new IndexOutOfBoundsException(
          String.format("readerIndex(%d) + length(%d) exceeds writerIndex(%d)", readerIndex, length, writerIndex));
      }
    }
  }

  /**
   * Read the byte at readerIndex.
   * @return byte value
   */
  public byte readByte() {
    ensureReadable(1);
    final byte b = getByte(readerIndex);
    ++readerIndex;
    return b;
  }

  public void readBytes(byte[] dst) {
    Preconditions.checkArgument(dst != null, "expecting valid dst bytearray");
    ensureReadable(dst.length);
    getBytes(readerIndex, dst, 0, dst.length);
  }

  /**
   * Set the provided byte value at the writerIndex.
   * @param value value to set
   */
  public void writeByte(byte value) {
    ensureWritable(1);
    PlatformDependent.putByte(addr(writerIndex), value);
    ++writerIndex;
  }

  /**
   * Set the lower order byte for the provided value at
   * the writerIndex.
   * @param value value to be set
   */
  public void writeByte(int value) {
    ensureWritable(1);
    PlatformDependent.putByte(addr(writerIndex), (byte)value);
    ++writerIndex;
  }

  public void writeBytes(byte[] src) {
    Preconditions.checkArgument(src != null, "expecting valid src array");
    writeBytes(src, 0, src.length);
  }

  public void writeBytes(byte[] src, int srcIndex, int length) {
    ensureWritable(length);
    setBytes(writerIndex, src, srcIndex, length);
    writerIndex += length;
  }

  /**
   * Set the provided int value as short at the writerIndex.
   * @param value value to set
   */
  public void writeShort(int value) {
    ensureWritable(SHORT_SIZE);
    PlatformDependent.putShort(addr(writerIndex), (short) value);
    writerIndex += SHORT_SIZE;
  }

  /**
   * Set the provided int value at the writerIndex.
   * @param value value to set
   */
  public void writeInt(int value) {
    ensureWritable(INT_SIZE);
    PlatformDependent.putInt(addr(writerIndex), value);
    writerIndex += INT_SIZE;
  }

  /**
   * Set the provided long value at the writerIndex.
   * @param value value to set
   */
  public void writeLong(long value) {
    ensureWritable(LONG_SIZE);
    PlatformDependent.putLong(addr(writerIndex), value);
    writerIndex += LONG_SIZE;
  }

  /**
   * Set the provided float value at the writerIndex.
   * @param value value to set
   */
  public void writeFloat(float value) {
    ensureWritable(FLOAT_SIZE);
    PlatformDependent.putInt(addr(writerIndex), Float.floatToRawIntBits(value));
    writerIndex += FLOAT_SIZE;
  }

  /**
   * Set the provided double value at the writerIndex.
   * @param value value to set
   */
  public void writeDouble(double value) {
    ensureWritable(DOUBLE_SIZE);
    PlatformDependent.putLong(addr(writerIndex), Double.doubleToRawLongBits(value));
    writerIndex += DOUBLE_SIZE;
  }

  /*--------------------------------------------------*
   | Following are another set of data set/get APIs   |
   | that read and write stream of bytes from/to byte |
   | arrays, ByteBuffer etc                           |
   |                                                  |
   *--------------------------------------------------*/

  private void checkIndex(int index, int fieldLength) {
    // check reference count
    this.ensureAccessible();
    // check bounds
    if (MathUtil.isOutOfBounds(index, fieldLength, this.capacity())) {
      throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))",
        index, fieldLength, this.capacity()));
    }
  }

  /**
   * Copy data from this ArrowBuf at a given index in into destination
   * byte array.
   * @param index starting index (0 based relative to the portion of memory)
   *              this ArrowBuf has access to
   * @param dst byte array to copy the data into
   */
  public void getBytes(int index, byte[] dst) {
    getBytes(index, dst, 0, dst.length);
  }

  /**
   * Copy data from this ArrowBuf at a given index into destination byte array.
   * @param index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param dst byte array to copy the data into
   * @param dstIndex starting index in dst byte array to copy into
   * @param length length of data to copy from this ArrowBuf
   */
  public void getBytes(int index, byte[] dst, int dstIndex, int length) {
    // bound check for this ArrowBuf where the data will be copied from
    checkIndex(index, length);
    // null check
    Preconditions.checkArgument(dst != null, "expecting a valid dst byte array");
    // bound check for dst byte array where the data will be copied to
    if (MathUtil.isOutOfBounds(dstIndex, length, dst.length)) {
      // not enough space to copy "length" bytes into dst array from dstIndex onwards
      throw new IndexOutOfBoundsException("Not enough space to copy data into destination" + dstIndex);
    }
    if (length != 0) {
      // copy "length" bytes from this ArrowBuf starting at addr(index) address
      // into dst byte array at dstIndex onwards
      PlatformDependent.copyMemory(addr(index), dst, dstIndex, (long)length);
    }
  }

  /**
   * Copy data from a given byte array starting at the given source index into
   * this ArrowBuf at a given index.
   * @param index index (0 based relative to the portion of memory this ArrowBuf
   *              has access to)
   * @param src src byte array to copy the data from
   * @param srcIndex index in the byte array where the copy will start from
   * @param length length of data to copy from byte array
   */
  public void setBytes(int index, byte[] src, int srcIndex, int length) {
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    // null check
    Preconditions.checkArgument(src != null, "expecting a valid src byte array");
    // bound check for src byte array where the data will be copied from
    if (MathUtil.isOutOfBounds(srcIndex, length, src.length)) {
      // not enough space to copy "length" bytes into dst array from dstIndex onwards
      throw new IndexOutOfBoundsException("Not enough space to copy data from byte array" + srcIndex);
    }
    if (length != 0) {
      // copy "length" bytes from src byte array at the starting index (srcIndex)
      // into this ArrowBuf starting at address "addr(index)"
      PlatformDependent.copyMemory(src, srcIndex, addr(index), (long)length);
    }
  }

  public void setBytes(int index, byte[] src) {
    Preconditions.checkArgument(src != null, "expecting valid src bytearray");
    setBytes(index, src, 0, src.length);
  }

  /**
   * Copy data from this ArrowBuf at a given index into the destination
   * ByteBuffer.
   * @param index index (0 based relative to the portion of memory this ArrowBuf
   *              has access to)
   * @param dst dst ByteBuffer where the data will be copied into
   */
  public void getBytes(int index, ByteBuffer dst) {
    // bound check for this ArrowBuf where the data will be copied from
    checkIndex(index, dst.remaining());
    int length = dst.remaining();
    if (dst.remaining() != 0) {
      final long srcAddress = addr(index);
      if (dst.isDirect()) {
        if (dst.isReadOnly()) {
          throw new ReadOnlyBufferException();
        }
        // copy dst.remaining() bytes of data from this ArrowBuf starting
        // at address srcAddress into the dst ByteBuffer starting at
        // address dstAddress
        final long dstAddress = PlatformDependent.directBufferAddress(dst) + (long)dst.position();
        PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
        // after copy, bump the next write position for the dst ByteBuffer
        dst.position(dst.position() + length);
      } else if (dst.hasArray()) {
        // copy dst.remaining() bytes of data from this ArrowBuf starting
        // at address srcAddress into the dst ByteBuffer starting at
        // index dstIndex
        final int dstIndex = dst.arrayOffset() + dst.position();
        PlatformDependent.copyMemory(srcAddress, dst.array(), dstIndex, (long)length);
        dst.position(dst.position() + length);
      } else {
        throw new UnsupportedOperationException("Copy from this ByteBuffer is not supported");
      }
    }
  }

  /**
   * Copy data into this ArrowBuf at a given index onwards from
   * a source ByteBuffer.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param src src ByteBuffer where the data will be copied from
   */
  public void setBytes(int index, ByteBuffer src) {
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, src.remaining());
    int length = src.remaining();
    long dstAddress = addr(index);
    if (length != 0) {
      if (src.isDirect()) {
        // copy src.remaining() bytes of data from src ByteBuffer starting at
        // address srcAddress into this ArrowBuf starting at address dstAddress
        final long srcAddress = PlatformDependent.directBufferAddress(src) + (long)src.position();
        PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
        // after copy, bump the next read position for the src ByteBuffer
        src.position(src.position() + length);
      } else if (src.hasArray()) {
        // copy src.remaining() bytes of data from src ByteBuffer starting at
        // index srcIndex into this ArrowBuf starting at address dstAddress
        final int srcIndex = src.arrayOffset() + src.position();
        PlatformDependent.copyMemory(src.array(), srcIndex, dstAddress, (long)length);
        // after copy, bump the next read position for the src ByteBuffer
        src.position(src.position() + length);
      } else {
        // word-wise memcpy
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
  }

  /**
   * Copy data into this ArrowBuf at a given index onwards from
   * a source ByteBuffer starting at a given srcIndex for a certain
   * length.
   * @param index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param src src ByteBuffer where the data will be copied from
   * @param srcIndex starting index in the src ByteBuffer where the data copy
   *                 will start from
   * @param length length of data to copy from src ByteBuffer
   */
  public void setBytes(int index, ByteBuffer src, int srcIndex, int length) {
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    if (src.isDirect()) {
      // copy length bytes of data from src ByteBuffer starting at address
      // srcAddress into this ArrowBuf at address dstAddres
      final long srcAddress = PlatformDependent.directBufferAddress(src) + (long)srcIndex;
      final long dstAddress = addr(index);
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
  }

  /**
   * Copy a given length of data from this ArrowBuf starting at a given index
   * into a dst ArrowBuf at dstIndex.
   * @param index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param dst dst ArrowBuf where the data will be copied into
   * @param dstIndex index (0 based relative to the portion of memory
   *              dst ArrowBuf has access to)
   * @param length length of data to copy
   */
  public void getBytes(int index, ArrowBuf dst, int dstIndex, int length) {
    // bound check for this ArrowBuf where the data will be copied from
    checkIndex(index, length);
    // bound check for this ArrowBuf where the data will be copied into
    Preconditions.checkArgument(dst != null, "expecting a valid ArrowBuf");
    // bound check for dst ArrowBuf
    if (MathUtil.isOutOfBounds(dstIndex, length, dst.capacity())) {
      throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))",
        dstIndex, length, dst.capacity()));
    }
    if (length != 0) {
      // copy length bytes of data from this ArrowBuf starting at
      // address srcAddress into dst ArrowBuf starting at address
      // dstAddress
      final long srcAddress = addr(index);
      final long dstAddress = dst.memoryAddress() + (long)dstIndex;
      PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
    }
  }

  /**
   * Copy data from src ArrowBuf starting at index srcIndex into this
   * ArrowBuf at given index.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param src src ArrowBuf where the data will be copied from
   * @param srcIndex starting index in the src ArrowBuf where the copy
   *                 will begin from
   * @param length length of data to copy from src ArrowBuf
   */
  public void setBytes(int index, ArrowBuf src, int srcIndex, int length) {
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    // null check
    Preconditions.checkArgument(src != null, "expecting a valid ArrowBuf");
    // bound check for src ArrowBuf
    if (MathUtil.isOutOfBounds(srcIndex, length, src.capacity())) {
      throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))",
        index, length, src.capacity()));
    }
    if (length != 0) {
      // copy length bytes of data from src ArrowBuf starting at
      // address srcAddress into this ArrowBuf starting at address
      // dstAddress
      final long srcAddress = src.memoryAddress() + (long)srcIndex;
      final long dstAddress = addr(index);
      PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
    }
  }

  /**
   * Copy readableBytes() number of bytes from src ArrowBuf
   * starting from its readerIndex into this ArrowBuf starting
   * at the given index.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param src src ArrowBuf where the data will be copied from
   */
  public void setBytes(int index, ArrowBuf src) {
    final int length = src.readableBytes();
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    // null check
    Preconditions.checkArgument(src != null, "expecting valid ArrowBuf");
    final long srcAddress = src.memoryAddress() + (long)src.readerIndex;
    final long dstAddress = addr(index);
    PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
    src.readerIndex(src.readerIndex + length);
  }

  public int setBytes(int index, InputStream in, int length) throws IOException {
    Preconditions.checkArgument(in != null, "expecting valid input stream");
    checkIndex(index, length);
    byte[] tmp = new byte[length];
    int readBytes = in.read(tmp);
    if (readBytes > 0) {
      PlatformDependent.copyMemory(tmp, 0, addr(index), readBytes);
    }
    return readBytes;
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

  public ArrowBuf setZero(int index, int length) {
    if (length == 0) {
      return this;
    } else {
      this.checkIndex(index, length);
      int nLong = length >>> 3;
      int nBytes = length & 7;

      int i;
      for (i = nLong; i > 0; --i) {
        setLong(index, 0L);
        index += 8;
      }

      if (nBytes == 4) {
        setInt(index, 0);
      } else if (nBytes < 4) {
        for (i = nBytes; i > 0; --i) {
          setByte(index, 0);
          ++index;
        }
      } else {
        setInt(index, 0);
        index += 4;
        for (i = nBytes - 4; i > 0; --i) {
          setByte(index, 0);
          ++index;
        }
      }

      return this;
    }
  }
}

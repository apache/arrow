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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.AllocationManager;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BaseAllocator.Verbosity;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.BufferLedger;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.util.Preconditions;

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
  private final BufferManager bufferManager;
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
      final BufferManager bufferManager,
      final int length,
      final long memoryAddress,
      boolean isEmpty) {
    this.referenceManager = referenceManager;
    this.bufferManager = bufferManager;
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
      throw new IllegalStateException("Ref count should be >= 1 for accessing the ArrowBuf");
    }
  }

  /**
   * Get a wrapper buffer to comply with Netty interfaces and
   * can be used in RPC/RPC allocator code.
   * @return netty compliant {@link NettyArrowBuf}
   */
  public NettyArrowBuf asNettyBuffer() {

    final NettyArrowBuf nettyArrowBuf = new NettyArrowBuf(
            this,
            isEmpty ? null : referenceManager.getAllocator().getAsByteBufAllocator(),
            length);
    nettyArrowBuf.readerIndex(readerIndex);
    nettyArrowBuf.writerIndex(writerIndex);
    return nettyArrowBuf;
  }

  /**
   * Get reference manager for this ArrowBuf.
   * @return user provided implementation of {@link ReferenceManager}
   */
  public ReferenceManager getReferenceManager() {
    return referenceManager;
  }

  public boolean isEmpty() {
    return isEmpty;
  }

  public int capacity() {
    return length;
  }

  /**
   * Adjusts the capacity of this buffer.  Size increases are NOT supported.
   *
   * @param newCapacity Must be in in the range [0, length).
   */
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

  /**
   * Returns the byte order of elements in this buffer.
   */
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  /**
   * Returns the number of bytes still available to read in this buffer.
   */
  public int readableBytes() {
    Preconditions.checkState(writerIndex >= readerIndex,
            "Writer index cannot be less than reader index");
    return writerIndex - readerIndex;
  }

  /**
   * Returns the number of bytes still available to write into this buffer before capacity is reached.
   */
  public int writableBytes() {
    return capacity() - writerIndex;
  }

  /**
   * Returns a slice of only the readable bytes in the buffer.
   */
  public ArrowBuf slice() {
    return slice(readerIndex, readableBytes());
  }

  /**
   *  Returns a slice (view) starting at <code>index</code> with the given <code>length</code>.
   */
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
    return isEmpty ? ByteBuffer.allocateDirect(0) : asNettyBuffer().nioBuffer();
  }

  public ByteBuffer nioBuffer(int index, int length) {
    return isEmpty ? ByteBuffer.allocateDirect(0) : asNettyBuffer().nioBuffer(index, length);
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
    Preconditions.checkArgument(fieldLength >= 0, "expecting non-negative data length");
    if (index < 0 || index > capacity() - fieldLength) {
      if (BaseAllocator.DEBUG) {
        historicalLog.logHistory(logger);
      }
      throw new IndexOutOfBoundsException(String.format(
        "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
    }
  }

  /**
   * Get long value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 8 byte long value
   */
  public long getLong(int index) {
    chk(index, LONG_SIZE);
    return PlatformDependent.getLong(addr(index));
  }

  /**
   * Set long value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setLong(int index, long value) {
    chk(index, LONG_SIZE);
    PlatformDependent.putLong(addr(index), value);
  }

  /**
   * Get float value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 4 byte float value
   */
  public float getFloat(int index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Set float value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setFloat(int index, float value) {
    chk(index, FLOAT_SIZE);
    PlatformDependent.putInt(addr(index), Float.floatToRawIntBits(value));
  }

  /**
   * Get double value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 8 byte double value
   */
  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Set double value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setDouble(int index, double value) {
    chk(index, DOUBLE_SIZE);
    PlatformDependent.putLong(addr(index), Double.doubleToRawLongBits(value));
  }

  /**
   * Get char value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 2 byte char value
   */
  public char getChar(int index) {
    return (char) getShort(index);
  }

  /**
   * Set char value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setChar(int index, int value) {
    chk(index, SHORT_SIZE);
    PlatformDependent.putShort(addr(index), (short) value);
  }

  /**
   * Get int value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 4 byte int value
   */
  public int getInt(int index) {
    chk(index, INT_SIZE);
    return PlatformDependent.getInt(addr(index));
  }

  /**
   * Set int value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setInt(int index, int value) {
    chk(index, INT_SIZE);
    PlatformDependent.putInt(addr(index), value);
  }

  /**
   * Get short value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 2 byte short value
   */
  public short getShort(int index) {
    chk(index, SHORT_SIZE);
    return PlatformDependent.getShort(addr(index));
  }

  /**
   * Set short value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setShort(int index, int value) {
    setShort(index, (short)value);
  }

  /**
   * Set short value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setShort(int index, short value) {
    chk(index, SHORT_SIZE);
    PlatformDependent.putShort(addr(index), value);
  }

  /**
   * Set byte value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setByte(int index, int value) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), (byte) value);
  }

  /**
   * Set byte value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setByte(int index, byte value) {
    chk(index, 1);
    PlatformDependent.putByte(addr(index), value);
  }

  /**
   * Get byte value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return byte value
   */
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

  /**
   * Read dst.length bytes at readerIndex into dst byte array
   * @param dst byte array where the data will be written
   */
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

  /**
   * Write the bytes from given byte array into this
   * ArrowBuf starting at writerIndex.
   * @param src src byte array
   */
  public void writeBytes(byte[] src) {
    Preconditions.checkArgument(src != null, "expecting valid src array");
    writeBytes(src, 0, src.length);
  }

  /**
   * Write the bytes from given byte array starting at srcIndex
   * into this ArrowBuf starting at writerIndex.
   * @param src src byte array
   * @param srcIndex index in the byte array where the copy will being from
   * @param length length of data to copy
   */
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
   | arrays, ByteBuffer, ArrowBuf etc                 |
   |                                                  |
   *--------------------------------------------------*/

  /**
   * Determine if the requested {@code index} and {@code length} will fit within {@code capacity}.
   * @param index The starting index.
   * @param length The length which will be utilized (starting from {@code index}).
   * @param capacity The capacity that {@code index + length} is allowed to be within.
   * @return {@code true} if the requested {@code index} and {@code length} will fit within {@code capacity}.
   * {@code false} if this would result in an index out of bounds exception.
   */
  private static boolean isOutOfBounds(int index, int length, int capacity) {
    return (index | length | (index + length) | (capacity - (index + length))) < 0;
  }

  private void checkIndex(int index, int fieldLength) {
    // check reference count
    this.ensureAccessible();
    // check bounds
    if (isOutOfBounds(index, fieldLength, this.capacity())) {
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
    if (isOutOfBounds(dstIndex, length, dst.length)) {
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
   * Copy data from a given byte array into this ArrowBuf starting at
   * a given index.
   * @param index starting index (0 based relative to the portion of memory)
   *              this ArrowBuf has access to
   * @param src byte array to copy the data from
   */
  public void setBytes(int index, byte[] src) {
    setBytes(index, src, 0, src.length);
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
    if (isOutOfBounds(srcIndex, length, src.length)) {
      // not enough space to copy "length" bytes into dst array from dstIndex onwards
      throw new IndexOutOfBoundsException("Not enough space to copy data from byte array" + srcIndex);
    }
    if (length > 0) {
      // copy "length" bytes from src byte array at the starting index (srcIndex)
      // into this ArrowBuf starting at address "addr(index)"
      PlatformDependent.copyMemory(src, srcIndex, addr(index), (long)length);
    }
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
    // dst.remaining() bytes of data will be copied into dst ByteBuffer
    if (dst.remaining() != 0) {
      // address in this ArrowBuf where the copy will begin from
      final long srcAddress = addr(index);
      if (dst.isDirect()) {
        if (dst.isReadOnly()) {
          throw new ReadOnlyBufferException();
        }
        // copy dst.remaining() bytes of data from this ArrowBuf starting
        // at address srcAddress into the dst ByteBuffer starting at
        // address dstAddress
        final long dstAddress = PlatformDependent.directBufferAddress(dst) + (long)dst.position();
        PlatformDependent.copyMemory(srcAddress, dstAddress, (long)dst.remaining());
        // after copy, bump the next write position for the dst ByteBuffer
        dst.position(dst.position() + dst.remaining());
      } else if (dst.hasArray()) {
        // copy dst.remaining() bytes of data from this ArrowBuf starting
        // at address srcAddress into the dst ByteBuffer starting at
        // index dstIndex
        final int dstIndex = dst.arrayOffset() + dst.position();
        PlatformDependent.copyMemory(srcAddress, dst.array(), dstIndex, (long)dst.remaining());
        // after copy, bump the next write position for the dst ByteBuffer
        dst.position(dst.position() + dst.remaining());
      } else {
        throw new UnsupportedOperationException("Copy from this ArrowBuf to ByteBuffer is not supported");
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
    // length of data to copy
    int length = src.remaining();
    // address in this ArrowBuf where the data will be copied to
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
        // copy word at a time
        while (length >= LONG_SIZE) {
          PlatformDependent.putLong(dstAddress, src.getLong());
          length -= LONG_SIZE;
          dstAddress += LONG_SIZE;
        }
        // copy last byte
        while (length > 0) {
          PlatformDependent.putByte(dstAddress, src.get());
          --length;
          ++dstAddress;
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
      PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
    } else {
      if (srcIndex == 0 && src.capacity() == length) {
        // copy the entire ByteBuffer from start to end of length
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
    if (isOutOfBounds(dstIndex, length, dst.capacity())) {
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
    if (isOutOfBounds(srcIndex, length, src.capacity())) {
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
    // null check
    Preconditions.checkArgument(src != null, "expecting valid ArrowBuf");
    final int length = src.readableBytes();
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    final long srcAddress = src.memoryAddress() + (long)src.readerIndex;
    final long dstAddress = addr(index);
    PlatformDependent.copyMemory(srcAddress, dstAddress, (long)length);
    src.readerIndex(src.readerIndex + length);
  }

  /**
   * Copy a certain length of bytes from given InputStream
   * into this ArrowBuf at the provided index.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param in src stream to copy from
   * @param length length of data to copy
   * @return number of bytes copied from stream into ArrowBuf
   * @throws IOException on failing to read from stream
   */
  public int setBytes(int index, InputStream in, int length) throws IOException {
    Preconditions.checkArgument(in != null, "expecting valid input stream");
    checkIndex(index, length);
    int readBytes = 0;
    if (length > 0) {
      byte[] tmp = new byte[length];
      // read the data from input stream into tmp byte array
      readBytes = in.read(tmp);
      if (readBytes > 0) {
        // copy readBytes length of data from the tmp byte array starting
        // at srcIndex 0 into this ArrowBuf starting at address addr(index)
        PlatformDependent.copyMemory(tmp, 0, addr(index), readBytes);
      }
    }
    return readBytes;
  }

  /**
   * Copy a certain length of bytes from this ArrowBuf at a given
   * index into the given OutputStream.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param out dst stream to copy data into
   * @param length length of data to copy
   * @throws IOException on failing to write to stream
   */
  public void getBytes(int index, OutputStream out, int length) throws IOException {
    Preconditions.checkArgument(out != null, "expecting valid output stream");
    checkIndex(index, length);
    if (length > 0) {
      // copy length bytes of data from this ArrowBuf starting at
      // address addr(index) into the tmp byte array starting at index 0
      byte[] tmp = new byte[length];
      PlatformDependent.copyMemory(addr(index), tmp, 0, length);
      // write the copied data to output stream
      out.write(tmp);
    }
  }

  @Override
  public void close() {
    referenceManager.release();
  }

  /**
   * Returns the possible memory consumed by this ArrowBuf in the worse case scenario.
   * (not shared, connected to larger underlying buffer of allocated memory)
   * @return Size in bytes.
   */
  public int getPossibleMemoryConsumed() {
    return isEmpty ? 0 : referenceManager.getSize();
  }

  /**
   * Return that is Accounted for by this buffer (and its potentially shared siblings within the
   * context of the associated allocator).
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
   * @return integer id
   */
  public long getId() {
    return id;
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

  /**
   * Get the index at which the next byte will be read from.
   * @return reader index
   */
  public int readerIndex() {
    return readerIndex;
  }

  /**
   * Get the index at which next byte will be written to.
   * @return writer index
   */
  public int writerIndex() {
    return writerIndex;
  }

  /**
   * Set the reader index for this ArrowBuf.
   * @param readerIndex new reader index
   * @return this ArrowBuf
   */
  public ArrowBuf readerIndex(int readerIndex) {
    this.readerIndex = readerIndex;
    return this;
  }

  /**
   * Set the writer index for this ArrowBuf.
   * @param writerIndex new writer index
   * @return this ArrowBuf
   */
  public ArrowBuf writerIndex(int writerIndex) {
    this.writerIndex = writerIndex;
    return this;
  }

  /**
   * Zero-out the bytes in this ArrowBuf starting at
   * the given index for the given length.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param length length of bytes to zero-out
   * @return this ArrowBuf
   */
  public ArrowBuf setZero(int index, int length) {
    if (length != 0) {
      this.checkIndex(index, length);
      PlatformDependent.setMemory(this.addr + index, length, (byte) 0);
    }
    return this;
  }

  /**
   * Returns <code>this</code> if size is less then {@link #capacity()}, otherwise
   * delegates to {@link BufferManager#replace(ArrowBuf, int)} to get a new buffer.
   */
  public ArrowBuf reallocIfNeeded(final int size) {
    Preconditions.checkArgument(size >= 0, "reallocation size must be non-negative");
    if (this.capacity() >= size) {
      return this;
    }
    if (bufferManager != null) {
      return bufferManager.replace(this, size);
    } else {
      throw new UnsupportedOperationException(
              "Realloc is only available in the context of operator's UDFs");
    }
  }

  /**
   * Following are wrapper methods to keep this backward compatible.
   */
  @Deprecated
  public void release() {
    referenceManager.release();
  }

  @Deprecated
  public void release(int decrement) {
    referenceManager.release(decrement);
  }

  @Deprecated
  public void retain() {
    referenceManager.retain();
  }

  @Deprecated
  public void retain(int increment) {
    referenceManager.retain(increment);
  }

  @Deprecated
  public ArrowBuf clear() {
    this.readerIndex = this.writerIndex = 0;
    return this;
  }

  /**
   * Initialize the reader and writer index.
   * @param readerIndex index to read from
   * @param writerIndex index to write to
   * @return this
   */
  @Deprecated
  public ArrowBuf setIndex(int readerIndex, int writerIndex) {
    if (readerIndex >= 0 && readerIndex <= writerIndex && writerIndex <= this.capacity()) {
      this.readerIndex = readerIndex;
      this.writerIndex = writerIndex;
      return this;
    } else {
      throw new IndexOutOfBoundsException(String.format("readerIndex: %d, writerIndex: %d " +
       "(expected:0 <= readerIndex <= writerIndex <= capacity(%d))", readerIndex, writerIndex, this.capacity()));
    }
  }
}

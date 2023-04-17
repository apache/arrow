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

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BaseAllocator.Verbosity;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.memory.util.HistoricalLog;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;

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
 * The management (allocation, deallocation, reference counting etc) for
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
  private long readerIndex;
  private long writerIndex;
  private final HistoricalLog historicalLog = BaseAllocator.DEBUG ?
          new HistoricalLog(BaseAllocator.DEBUG_LOG_LENGTH, "ArrowBuf[%d]", id) : null;
  private volatile long capacity;

  /**
   * Constructs a new ArrowBuf.
   *
   * @param referenceManager The memory manager to track memory usage and reference count of this buffer
   * @param capacity The capacity in bytes of this buffer
   */
  public ArrowBuf(
      final ReferenceManager referenceManager,
      final BufferManager bufferManager,
      final long capacity,
      final long memoryAddress) {
    this.referenceManager = referenceManager;
    this.bufferManager = bufferManager;
    this.addr = memoryAddress;
    this.capacity = capacity;
    this.readerIndex = 0;
    this.writerIndex = 0;
    if (BaseAllocator.DEBUG) {
      historicalLog.recordEvent("create()");
    }
  }

  public int refCnt() {
    return referenceManager.getRefCount();
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
  public void checkBytes(long start, long end) {
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
   * Get reference manager for this ArrowBuf.
   * @return user provided implementation of {@link ReferenceManager}
   */
  public ReferenceManager getReferenceManager() {
    return referenceManager;
  }

  public long capacity() {
    return capacity;
  }

  /**
   * Adjusts the capacity of this buffer.  Size increases are NOT supported.
   *
   * @param newCapacity Must be in in the range [0, length).
   */
  public synchronized ArrowBuf capacity(long newCapacity) {

    if (newCapacity == capacity) {
      return this;
    }

    Preconditions.checkArgument(newCapacity >= 0);

    if (newCapacity < capacity) {
      capacity = newCapacity;
      return this;
    }

    throw new UnsupportedOperationException("Buffers don't support resizing that increases the size.");
  }

  /**
   * Returns the byte order of elements in this buffer.
   */
  public ByteOrder order() {
    return ByteOrder.nativeOrder();
  }

  /**
   * Returns the number of bytes still available to read in this buffer.
   */
  public long readableBytes() {
    Preconditions.checkState(writerIndex >= readerIndex,
            "Writer index cannot be less than reader index");
    return writerIndex - readerIndex;
  }

  /**
   * Returns the number of bytes still available to write into this buffer before capacity is reached.
   */
  public long writableBytes() {
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
  public ArrowBuf slice(long index, long length) {

    Preconditions.checkPositionIndex(index, this.capacity);
    Preconditions.checkPositionIndex(index + length, this.capacity);

    /*
     * Re the behavior of reference counting, see http://netty.io/wiki/reference-counted-objects
     * .html#wiki-h3-5, which
     * explains that derived buffers share their reference count with their parent
     */
    final ArrowBuf newBuf = referenceManager.deriveBuffer(this, index, length);
    newBuf.writerIndex(length);
    return newBuf;
  }

  /**
   * Make a nio byte buffer from this arrowbuf.
   */
  public ByteBuffer nioBuffer() {
    return nioBuffer(readerIndex, checkedCastToInt(readableBytes()));
  }


  /**
   *  Make a nio byte buffer from this ArrowBuf.
   */
  public ByteBuffer nioBuffer(long index, int length) {
    chk(index, length);
    return getDirectBuffer(index, length);
  }

  private ByteBuffer getDirectBuffer(long index, int length) {
    long address = addr(index);
    return MemoryUtil.directBuffer(address, length);
  }

  public long memoryAddress() {
    return this.addr;
  }

  @Override
  public String toString() {
    return String.format("ArrowBuf[%d], address:%d, capacity:%d", id, memoryAddress(), capacity);
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
   * @return the absolute address within the memory
   */
  private long addr(long index) {
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
  private void chk(long index, long length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      checkIndexD(index, length);
    }
  }

  private void checkIndexD(long index, long fieldLength) {
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
  public long getLong(long index) {
    chk(index, LONG_SIZE);
    return MemoryUtil.UNSAFE.getLong(addr(index));
  }

  /**
   * Set long value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setLong(long index, long value) {
    chk(index, LONG_SIZE);
    MemoryUtil.UNSAFE.putLong(addr(index), value);
  }

  /**
   * Get float value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 4 byte float value
   */
  public float getFloat(long index) {
    return Float.intBitsToFloat(getInt(index));
  }

  /**
   * Set float value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setFloat(long index, float value) {
    chk(index, FLOAT_SIZE);
    MemoryUtil.UNSAFE.putInt(addr(index), Float.floatToRawIntBits(value));
  }

  /**
   * Get double value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 8 byte double value
   */
  public double getDouble(long index) {
    return Double.longBitsToDouble(getLong(index));
  }

  /**
   * Set double value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setDouble(long index, double value) {
    chk(index, DOUBLE_SIZE);
    MemoryUtil.UNSAFE.putLong(addr(index), Double.doubleToRawLongBits(value));
  }

  /**
   * Get char value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 2 byte char value
   */
  public char getChar(long index) {
    return (char) getShort(index);
  }

  /**
   * Set char value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setChar(long index, int value) {
    chk(index, SHORT_SIZE);
    MemoryUtil.UNSAFE.putShort(addr(index), (short) value);
  }

  /**
   * Get int value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 4 byte int value
   */
  public int getInt(long index) {
    chk(index, INT_SIZE);
    return MemoryUtil.UNSAFE.getInt(addr(index));
  }

  /**
   * Set int value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setInt(long index, int value) {
    chk(index, INT_SIZE);
    MemoryUtil.UNSAFE.putInt(addr(index), value);
  }

  /**
   * Get short value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return 2 byte short value
   */
  public short getShort(long index) {
    chk(index, SHORT_SIZE);
    return MemoryUtil.UNSAFE.getShort(addr(index));
  }

  /**
   * Set short value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setShort(long index, int value) {
    setShort(index, (short) value);
  }

  /**
   * Set short value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setShort(long index, short value) {
    chk(index, SHORT_SIZE);
    MemoryUtil.UNSAFE.putShort(addr(index), value);
  }

  /**
   * Set byte value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setByte(long index, int value) {
    chk(index, 1);
    MemoryUtil.UNSAFE.putByte(addr(index), (byte) value);
  }

  /**
   * Set byte value at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be written
   * @param value value to write
   */
  public void setByte(long index, byte value) {
    chk(index, 1);
    MemoryUtil.UNSAFE.putByte(addr(index), value);
  }

  /**
   * Get byte value stored at a particular index in the
   * underlying memory chunk this ArrowBuf has access to.
   * @param index index (0 based relative to this ArrowBuf)
   *              where the value will be read from
   * @return byte value
   */
  public byte getByte(long index) {
    chk(index, 1);
    return MemoryUtil.UNSAFE.getByte(addr(index));
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
    getBytes(readerIndex, dst, 0, checkedCastToInt(dst.length));
  }

  /**
   * Set the provided byte value at the writerIndex.
   * @param value value to set
   */
  public void writeByte(byte value) {
    ensureWritable(1);
    MemoryUtil.UNSAFE.putByte(addr(writerIndex), value);
    ++writerIndex;
  }

  /**
   * Set the lower order byte for the provided value at
   * the writerIndex.
   * @param value value to be set
   */
  public void writeByte(int value) {
    ensureWritable(1);
    MemoryUtil.UNSAFE.putByte(addr(writerIndex), (byte) value);
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
    MemoryUtil.UNSAFE.putShort(addr(writerIndex), (short) value);
    writerIndex += SHORT_SIZE;
  }

  /**
   * Set the provided int value at the writerIndex.
   * @param value value to set
   */
  public void writeInt(int value) {
    ensureWritable(INT_SIZE);
    MemoryUtil.UNSAFE.putInt(addr(writerIndex), value);
    writerIndex += INT_SIZE;
  }

  /**
   * Set the provided long value at the writerIndex.
   * @param value value to set
   */
  public void writeLong(long value) {
    ensureWritable(LONG_SIZE);
    MemoryUtil.UNSAFE.putLong(addr(writerIndex), value);
    writerIndex += LONG_SIZE;
  }

  /**
   * Set the provided float value at the writerIndex.
   * @param value value to set
   */
  public void writeFloat(float value) {
    ensureWritable(FLOAT_SIZE);
    MemoryUtil.UNSAFE.putInt(addr(writerIndex), Float.floatToRawIntBits(value));
    writerIndex += FLOAT_SIZE;
  }

  /**
   * Set the provided double value at the writerIndex.
   * @param value value to set
   */
  public void writeDouble(double value) {
    ensureWritable(DOUBLE_SIZE);
    MemoryUtil.UNSAFE.putLong(addr(writerIndex), Double.doubleToRawLongBits(value));
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
  private static boolean isOutOfBounds(long index, long length, long capacity) {
    return (index | length | (index + length) | (capacity - (index + length))) < 0;
  }

  private void checkIndex(long index, long fieldLength) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      // check reference count
      this.ensureAccessible();
      // check bounds
      if (isOutOfBounds(index, fieldLength, this.capacity())) {
        throw new IndexOutOfBoundsException(String.format("index: %d, length: %d (expected: range(0, %d))",
                index, fieldLength, this.capacity()));
      }
    }
  }

  /**
   * Copy data from this ArrowBuf at a given index in into destination
   * byte array.
   * @param index starting index (0 based relative to the portion of memory)
   *              this ArrowBuf has access to
   * @param dst byte array to copy the data into
   */
  public void getBytes(long index, byte[] dst) {
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
  public void getBytes(long index, byte[] dst, int dstIndex, int length) {
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
      MemoryUtil.UNSAFE.copyMemory(null, addr(index), dst, MemoryUtil.BYTE_ARRAY_BASE_OFFSET + dstIndex, length);
    }
  }

  /**
   * Copy data from a given byte array into this ArrowBuf starting at
   * a given index.
   * @param index starting index (0 based relative to the portion of memory)
   *              this ArrowBuf has access to
   * @param src byte array to copy the data from
   */
  public void setBytes(long index, byte[] src) {
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
  public void setBytes(long index, byte[] src, int srcIndex, long length) {
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
      MemoryUtil.UNSAFE.copyMemory(src, MemoryUtil.BYTE_ARRAY_BASE_OFFSET + srcIndex, null, addr(index), length);
    }
  }

  /**
   * Copy data from this ArrowBuf at a given index into the destination
   * ByteBuffer.
   * @param index index (0 based relative to the portion of memory this ArrowBuf
   *              has access to)
   * @param dst dst ByteBuffer where the data will be copied into
   */
  public void getBytes(long index, ByteBuffer dst) {
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
        final long dstAddress = MemoryUtil.getByteBufferAddress(dst) + dst.position();
        MemoryUtil.UNSAFE.copyMemory(null, srcAddress, null, dstAddress, dst.remaining());
        // after copy, bump the next write position for the dst ByteBuffer
        dst.position(dst.position() + dst.remaining());
      } else if (dst.hasArray()) {
        // copy dst.remaining() bytes of data from this ArrowBuf starting
        // at address srcAddress into the dst ByteBuffer starting at
        // index dstIndex
        final int dstIndex = dst.arrayOffset() + dst.position();
        MemoryUtil.UNSAFE.copyMemory(
                null, srcAddress, dst.array(), MemoryUtil.BYTE_ARRAY_BASE_OFFSET + dstIndex, dst.remaining());
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
  public void setBytes(long index, ByteBuffer src) {
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
        final long srcAddress = MemoryUtil.getByteBufferAddress(src) + src.position();
        MemoryUtil.UNSAFE.copyMemory(null, srcAddress, null, dstAddress, length);
        // after copy, bump the next read position for the src ByteBuffer
        src.position(src.position() + length);
      } else if (src.hasArray()) {
        // copy src.remaining() bytes of data from src ByteBuffer starting at
        // index srcIndex into this ArrowBuf starting at address dstAddress
        final int srcIndex = src.arrayOffset() + src.position();
        MemoryUtil.UNSAFE.copyMemory(
                src.array(), MemoryUtil.BYTE_ARRAY_BASE_OFFSET + srcIndex, null, dstAddress, length);
        // after copy, bump the next read position for the src ByteBuffer
        src.position(src.position() + length);
      } else {
        final ByteOrder originalByteOrder = src.order();
        src.order(order());
        try {
          // copy word at a time
          while (length - 128 >= LONG_SIZE) {
            for (int x = 0; x < 16; x++) {
              MemoryUtil.UNSAFE.putLong(dstAddress, src.getLong());
              length -= LONG_SIZE;
              dstAddress += LONG_SIZE;
            }
          }
          while (length >= LONG_SIZE) {
            MemoryUtil.UNSAFE.putLong(dstAddress, src.getLong());
            length -= LONG_SIZE;
            dstAddress += LONG_SIZE;
          }
          // copy last byte
          while (length > 0) {
            MemoryUtil.UNSAFE.putByte(dstAddress, src.get());
            --length;
            ++dstAddress;
          }
        } finally {
          src.order(originalByteOrder);
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
  public void setBytes(long index, ByteBuffer src, int srcIndex, int length) {
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    if (src.isDirect()) {
      // copy length bytes of data from src ByteBuffer starting at address
      // srcAddress into this ArrowBuf at address dstAddress
      final long srcAddress = MemoryUtil.getByteBufferAddress(src) + srcIndex;
      final long dstAddress = addr(index);
      MemoryUtil.UNSAFE.copyMemory(null, srcAddress, null, dstAddress, length);
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
  public void getBytes(long index, ArrowBuf dst, long dstIndex, int length) {
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
      final long dstAddress = dst.memoryAddress() + (long) dstIndex;
      MemoryUtil.UNSAFE.copyMemory(null, srcAddress, null, dstAddress, length);
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
  public void setBytes(long index, ArrowBuf src, long srcIndex, long length) {
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
      final long srcAddress = src.memoryAddress() + srcIndex;
      final long dstAddress = addr(index);
      MemoryUtil.UNSAFE.copyMemory(null, srcAddress, null, dstAddress, length);
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
  public void setBytes(long index, ArrowBuf src) {
    // null check
    Preconditions.checkArgument(src != null, "expecting valid ArrowBuf");
    final long length = src.readableBytes();
    // bound check for this ArrowBuf where the data will be copied into
    checkIndex(index, length);
    final long srcAddress = src.memoryAddress() + src.readerIndex;
    final long dstAddress = addr(index);
    MemoryUtil.UNSAFE.copyMemory(null, srcAddress, null, dstAddress, length);
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
  public int setBytes(long index, InputStream in, int length) throws IOException {
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
        MemoryUtil.UNSAFE.copyMemory(tmp, MemoryUtil.BYTE_ARRAY_BASE_OFFSET, null, addr(index), readBytes);
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
  public void getBytes(long index, OutputStream out, int length) throws IOException {
    Preconditions.checkArgument(out != null, "expecting valid output stream");
    checkIndex(index, length);
    if (length > 0) {
      // copy length bytes of data from this ArrowBuf starting at
      // address addr(index) into the tmp byte array starting at index 0
      byte[] tmp = new byte[length];
      MemoryUtil.UNSAFE.copyMemory(null, addr(index), tmp, MemoryUtil.BYTE_ARRAY_BASE_OFFSET, length);
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
  public long getPossibleMemoryConsumed() {
    return referenceManager.getSize();
  }

  /**
   * Return that is Accounted for by this buffer (and its potentially shared siblings within the
   * context of the associated allocator).
   * @return Size in bytes.
   */
  public long getActualMemoryConsumed() {
    return referenceManager.getAccountedSize();
  }

  /**
   * Return the buffer's byte contents in the form of a hex dump.
   *
   * @param start  the starting byte index
   * @param length how many bytes to log
   * @return A hex dump in a String.
   */
  public String toHexString(final long start, final int length) {
    final long roundedStart = (start / LOG_BYTES_PER_ROW) * LOG_BYTES_PER_ROW;

    final StringBuilder sb = new StringBuilder("buffer byte dump\n");
    long index = roundedStart;
    for (long nLogged = 0; nLogged < length; nLogged += LOG_BYTES_PER_ROW) {
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
   * Print information of this buffer into <code>sb</code> at the given
   * indentation and verbosity level.
   *
   * <p>It will include history if BaseAllocator.DEBUG is true and
   * the verbosity.includeHistoricalLog are true.
   *
   */
  @VisibleForTesting
  public void print(StringBuilder sb, int indent, Verbosity verbosity) {
    CommonUtil.indent(sb, indent).append(toString());

    if (BaseAllocator.DEBUG && verbosity.includeHistoricalLog) {
      sb.append("\n");
      historicalLog.buildHistory(sb, indent + 1, verbosity.includeStackTraces);
    }
  }

  /**
   * Print detailed information of this buffer into <code>sb</code>.
   *
   * <p>Most information will only be present if BaseAllocator.DEBUG is true.
   *
   */
  public void print(StringBuilder sb, int indent) {
    print(sb, indent, Verbosity.LOG_WITH_STACKTRACE);
  }

  /**
   * Get the index at which the next byte will be read from.
   * @return reader index
   */
  public long readerIndex() {
    return readerIndex;
  }

  /**
   * Get the index at which next byte will be written to.
   * @return writer index
   */
  public long writerIndex() {
    return writerIndex;
  }

  /**
   * Set the reader index for this ArrowBuf.
   * @param readerIndex new reader index
   * @return this ArrowBuf
   */
  public ArrowBuf readerIndex(long readerIndex) {
    this.readerIndex = readerIndex;
    return this;
  }

  /**
   * Set the writer index for this ArrowBuf.
   * @param writerIndex new writer index
   * @return this ArrowBuf
   */
  public ArrowBuf writerIndex(long writerIndex) {
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
  public ArrowBuf setZero(long index, long length) {
    if (length != 0) {
      this.checkIndex(index, length);
      MemoryUtil.UNSAFE.setMemory(this.addr + index, length, (byte) 0);
    }
    return this;
  }

  /**
   * Sets all bits to one in the specified range.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param length length of bytes to set.
   * @return this ArrowBuf
   * @deprecated use {@link ArrowBuf#setOne(long, long)} instead.
   */
  @Deprecated
  public ArrowBuf setOne(int index, int length) {
    if (length != 0) {
      this.checkIndex(index, length);
      MemoryUtil.UNSAFE.setMemory(this.addr + index, length, (byte) 0xff);
    }
    return this;
  }

  /**
   * Sets all bits to one in the specified range.
   * @param index index index (0 based relative to the portion of memory
   *              this ArrowBuf has access to)
   * @param length length of bytes to set.
   * @return this ArrowBuf
   */
  public ArrowBuf setOne(long index, long length) {
    if (length != 0) {
      this.checkIndex(index, length);
      MemoryUtil.UNSAFE.setMemory(this.addr + index, length, (byte) 0xff);
    }
    return this;
  }

  /**
   * Returns <code>this</code> if size is less then {@link #capacity()}, otherwise
   * delegates to {@link BufferManager#replace(ArrowBuf, long)} to get a new buffer.
   */
  public ArrowBuf reallocIfNeeded(final long size) {
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

  public ArrowBuf clear() {
    this.readerIndex = this.writerIndex = 0;
    return this;
  }
}

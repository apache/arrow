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

import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.ArrowByteBufAllocator;
import org.apache.arrow.memory.BoundsChecking;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

import io.netty.util.internal.PlatformDependent;

/**
 * Netty specific wrapper over ArrowBuf for use in Netty framework.
 */
public class NettyArrowBuf extends AbstractByteBuf implements AutoCloseable {

  private final ArrowBuf arrowBuf;
  private final ArrowByteBufAllocator arrowByteBufAllocator;
  private int length;
  private final long address;

  /**
   * Constructs a new instance.
   *
   * @param arrowBuf        The buffer to wrap.
   * @param bufferAllocator The allocator for the buffer.
   * @param length          The length of this buffer.
   */
  public NettyArrowBuf(
      final ArrowBuf arrowBuf,
      final BufferAllocator bufferAllocator,
      final int length) {
    super(length);
    this.arrowBuf = arrowBuf;
    this.arrowByteBufAllocator = new ArrowByteBufAllocator(bufferAllocator);
    this.length = length;
    this.address = arrowBuf.memoryAddress();
  }

  @Override
  public ByteBuf copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf retain() {
    arrowBuf.getReferenceManager().retain();
    return this;
  }

  public ArrowBuf arrowBuf() {
    return arrowBuf;
  }

  @Override
  public ByteBuf retain(final int increment) {
    arrowBuf.getReferenceManager().retain(increment);
    return this;
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public synchronized ByteBuf capacity(int newCapacity) {
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

  @Override
  public ByteBuf unwrap() {

    // According to Netty's ByteBuf interface, unwrap() should return null if the buffer cannot be unwrapped
    // https://github.com/netty/netty/blob/9fe796e10a433b6cd20ad78b2c39cd56b86ccd2e/buffer/src/main/java/io/netty/buffer/ByteBuf.java#L305

    // Throwing here breaks toString() in AbstractByteBuf
    // Since toString() is used to build debug / error messages, this can cause strange behavior

    return null;
  }

  @Override
  public int refCnt() {
    return arrowBuf.getReferenceManager().getRefCount();
  }

  @Override
  public ArrowByteBufAllocator alloc() {
    return arrowByteBufAllocator;
  }

  @Override
  public boolean hasArray() {
    return false;
  }

  @Override
  public byte[] array() {
    throw new UnsupportedOperationException("Operation not supported on direct buffer");
  }

  @Override
  public int arrayOffset() {
    throw new UnsupportedOperationException("Operation not supported on direct buffer");
  }

  @Override
  public boolean hasMemoryAddress() {
    return true;
  }

  @Override
  public long memoryAddress() {
    return this.address;
  }

  @Override
  public ByteBuf touch() {
    return this;
  }

  @Override
  public ByteBuf touch(Object hint) {
    return this;
  }

  @Override
  public int capacity() {
    return (int) Math.min(Integer.MAX_VALUE, arrowBuf.capacity());
  }

  @Override
  public NettyArrowBuf slice() {
    return unwrapBuffer(arrowBuf.slice(readerIndex, writerIndex - readerIndex));
  }

  @Override
  public NettyArrowBuf slice(int index, int length) {
    return unwrapBuffer(arrowBuf.slice(index, length));
  }

  @Override
  public void close() {
    arrowBuf.close();
  }

  @Override
  public boolean release() {
    return arrowBuf.getReferenceManager().release();
  }

  @Override
  public boolean release(int decrement) {
    return arrowBuf.getReferenceManager().release(decrement);
  }

  @Override
  public NettyArrowBuf readerIndex(int readerIndex) {
    super.readerIndex(readerIndex);
    return this;
  }

  @Override
  public NettyArrowBuf writerIndex(int writerIndex) {
    super.writerIndex(writerIndex);
    return this;
  }

  @Override
  public int nioBufferCount() {
    return 1;
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    ByteBuffer nioBuf = getDirectBuffer(index);
    // Follows convention from other ByteBuf implementations.
    return (ByteBuffer) nioBuf.clear().limit(length);
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    return new ByteBuffer[] {nioBuffer()};
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return new ByteBuffer[] {nioBuffer(index, length)};
  }

  @Override
  public ByteBuffer nioBuffer() {
    return nioBuffer(readerIndex(), readableBytes());
  }


  /**
   * Returns a buffer that is zero positioned but points
   * to a slice of the original buffer starting at given index.
   */
  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    chk(index, length);
    final ByteBuffer buffer = getDirectBuffer(index);
    buffer.limit(length);
    return buffer;
  }

  /**
   * Returns a buffer that is zero positioned but points
   * to a slice of the original buffer starting at given index.
   */
  public ByteBuffer nioBuffer(long index, int length) {
    chk(index, length);
    final ByteBuffer buffer = getDirectBuffer(index);
    buffer.limit(length);
    return buffer;
  }

  /**
   * Get this ArrowBuf as a direct {@link ByteBuffer}.
   *
   * @return ByteBuffer
   */
  private ByteBuffer getDirectBuffer(long index) {
    return PlatformDependent.directBuffer(addr(index), checkedCastToInt(length - index));
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    arrowBuf.getBytes(index, dst);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuffer src) {
    arrowBuf.setBytes(index, src);
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    arrowBuf.getBytes(index, dst, dstIndex, length);
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
    arrowBuf.setBytes(index, src, srcIndex, length);
    return this;
  }

  /**
   * Determine if the requested {@code index} and {@code length} will fit within {@code capacity}.
   *
   * @param index    The starting index.
   * @param length   The length which will be utilized (starting from {@code index}).
   * @param capacity The capacity that {@code index + length} is allowed to be within.
   * @return {@code true} if the requested {@code index} and {@code length} will fit within {@code capacity}.
   * {@code false} if this would result in an index out of bounds exception.
   */
  private static boolean isOutOfBounds(int index, int length, int capacity) {
    return (index | length | (index + length) | (capacity - (index + length))) < 0;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
    chk(index, length);
    Preconditions.checkArgument(dst != null, "Expecting valid dst ByteBuffer");
    if (isOutOfBounds(dstIndex, length, dst.capacity())) {
      throw new IndexOutOfBoundsException("dstIndex: " + dstIndex + " length: " + length);
    } else {
      final long srcAddress = addr(index);
      if (dst.hasMemoryAddress()) {
        final long dstAddress = dst.memoryAddress() + (long) dstIndex;
        PlatformDependent.copyMemory(srcAddress, dstAddress, (long) length);
      } else if (dst.hasArray()) {
        dstIndex += dst.arrayOffset();
        PlatformDependent.copyMemory(srcAddress, dst.array(), dstIndex, (long) length);
      } else {
        dst.setBytes(dstIndex, this, index, length);
      }
    }
    return this;
  }

  @Override
  public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
    chk(index, length);
    Preconditions.checkArgument(src != null, "Expecting valid src ByteBuffer");
    if (isOutOfBounds(srcIndex, length, src.capacity())) {
      throw new IndexOutOfBoundsException("srcIndex: " + srcIndex + " length: " + length);
    } else {
      if (length != 0) {
        final long dstAddress = addr(index);
        if (src.hasMemoryAddress()) {
          final long srcAddress = src.memoryAddress() + (long) srcIndex;
          PlatformDependent.copyMemory(srcAddress, dstAddress, (long) length);
        } else if (src.hasArray()) {
          srcIndex += src.arrayOffset();
          PlatformDependent.copyMemory(src.array(), srcIndex, dstAddress, (long) length);
        } else {
          src.getBytes(srcIndex, this, index, length);
        }
      }
    }
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    arrowBuf.getBytes(index, out, length);
    return this;
  }

  @Override
  public int setBytes(int index, InputStream in, int length) throws IOException {
    return arrowBuf.setBytes(index, in, length);
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    Preconditions.checkArgument(out != null, "expecting valid gathering byte channel");
    chk(index, length);
    if (length == 0) {
      return 0;
    } else {
      final ByteBuffer tmpBuf = getDirectBuffer(index);
      tmpBuf.clear().limit(length);
      return out.write(tmpBuf);
    }
  }

  @Override
  public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
    chk(index, length);
    if (length == 0) {
      return 0;
    } else {
      final ByteBuffer tmpBuf = getDirectBuffer(index);
      tmpBuf.clear().limit(length);
      return out.write(tmpBuf, position);
    }
  }

  @Override
  public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
    return (int) in.read(nioBuffers(index, length));
  }

  @Override
  public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
    return (int) in.read(nioBuffers(index, length));
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    return this;
  }

  @Override
  protected int _getUnsignedMedium(int index) {
    return getUnsignedMedium(index);
  }

  @Override
  protected int _getUnsignedMediumLE(int index) {
    this.chk(index, 3);
    long addr = this.addr(index);
    return PlatformDependent.getByte(addr) & 255 |
        (Short.reverseBytes(PlatformDependent.getShort(addr + 1L)) & '\uffff') << 8;
  }


  /*-------------------------------------------------*
   |                                                 |
   |            get() APIs                           |
   |                                                 |
   *-------------------------------------------------*/


  @Override
  protected byte _getByte(int index) {
    return getByte(index);
  }

  @Override
  public byte getByte(int index) {
    return arrowBuf.getByte(index);
  }

  @Override
  protected short _getShortLE(int index) {
    short s = getShort(index);
    return Short.reverseBytes(s);
  }

  @Override
  protected short _getShort(int index) {
    return getShort(index);
  }

  @Override
  public short getShort(int index) {
    return arrowBuf.getShort(index);
  }

  @Override
  protected int _getIntLE(int index) {
    int value = getInt(index);
    return Integer.reverseBytes(value);
  }

  @Override
  protected int _getInt(int index) {
    return getInt(index);
  }

  @Override
  public int getInt(int index) {
    return arrowBuf.getInt(index);
  }

  @Override
  protected long _getLongLE(int index) {
    long value = getLong(index);
    return Long.reverseBytes(value);
  }

  @Override
  protected long _getLong(int index) {
    return getLong(index);
  }

  @Override
  public long getLong(int index) {
    return arrowBuf.getLong(index);
  }


  /*-------------------------------------------------*
   |                                                 |
   |            set() APIs                           |
   |                                                 |
   *-------------------------------------------------*/


  @Override
  protected void _setByte(int index, int value) {
    setByte(index, value);
  }

  @Override
  public NettyArrowBuf setByte(int index, int value) {
    arrowBuf.setByte(index, value);
    return this;
  }

  @Override
  protected void _setShortLE(int index, int value) {
    this.chk(index, 2);
    PlatformDependent.putShort(this.addr(index), Short.reverseBytes((short) value));
  }

  @Override
  protected void _setShort(int index, int value) {
    setShort(index, value);
  }

  @Override
  public NettyArrowBuf setShort(int index, int value) {
    arrowBuf.setShort(index, value);
    return this;
  }

  private long addr(long index) {
    return address + index;
  }

  /**
   * Helper function to do bounds checking at a particular
   * index for particular length of data.
   *
   * @param index       index (0 based relative to this ArrowBuf)
   * @param fieldLength provided length of data for get/set
   */
  private void chk(long index, long fieldLength) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      // check reference count
      ensureAccessible();
      // check bounds
      if (fieldLength < 0) {
        throw new IllegalArgumentException("length: " + fieldLength + " (expected: >= 0)");
      }
      if (index < 0 || index > capacity() - fieldLength) {
        throw new IndexOutOfBoundsException(String.format(
            "index: %d, length: %d (expected: range(0, %d))", index, fieldLength, capacity()));
      }
    }
  }

  @Override
  protected void _setMedium(int index, int value) {
    setMedium(index, value);
  }

  @Override
  protected void _setMediumLE(int index, int value) {
    this.chk(index, 3);
    long addr = this.addr(index);
    PlatformDependent.putByte(addr, (byte) value);
    PlatformDependent.putShort(addr + 1L, Short.reverseBytes((short) (value >>> 8)));
  }

  @Override
  public NettyArrowBuf setMedium(int index, int value) {
    chk(index, 3);
    final long addr = addr(index);
    // we need to store 3 bytes starting from least significant byte
    // and ignoring the most significant byte
    // since arrow memory format is little endian, we will
    // first store the first 2 bytes followed by third byte
    // example: if the 4 byte int value is ABCD where A is MSB
    // D is LSB then we effectively want to store DCB in increasing
    // address to get Little Endian byte order
    // (short)value will give us CD and PlatformDependent.putShort()
    // will store them in LE order as DC starting at address addr
    // in order to get B, we do ABCD >>> 16 = 00AB => (byte)AB which
    // gives B. We store this at address addr + 2. So finally we get
    // DCB
    PlatformDependent.putShort(addr, (short) value);
    PlatformDependent.putByte(addr + 2, (byte) (value >>> 16));
    return this;
  }

  @Override
  protected void _setInt(int index, int value) {
    setInt(index, value);
  }

  @Override
  protected void _setIntLE(int index, int value) {
    this.chk(index, 4);
    PlatformDependent.putInt(this.addr(index), Integer.reverseBytes(value));
  }

  @Override
  public NettyArrowBuf setInt(int index, int value) {
    arrowBuf.setInt(index, value);
    return this;
  }

  @Override
  protected void _setLong(int index, long value) {
    setLong(index, value);
  }

  @Override
  public void _setLongLE(int index, long value) {
    this.chk(index, 8);
    PlatformDependent.putLong(this.addr(index), Long.reverseBytes(value));
  }

  @Override
  public NettyArrowBuf setLong(int index, long value) {
    arrowBuf.setLong(index, value);
    return this;
  }

  /**
   * unwrap arrow buffer into a netty buffer.
   */
  public static NettyArrowBuf unwrapBuffer(ArrowBuf buf) {
    final NettyArrowBuf nettyArrowBuf = new NettyArrowBuf(
        buf,
        buf.getReferenceManager().getAllocator(),
        checkedCastToInt(buf.capacity()));
    nettyArrowBuf.readerIndex(checkedCastToInt(buf.readerIndex()));
    nettyArrowBuf.writerIndex(checkedCastToInt(buf.writerIndex()));
    return nettyArrowBuf;
  }

}

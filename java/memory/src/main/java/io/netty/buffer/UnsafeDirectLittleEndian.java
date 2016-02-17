/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;

import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The underlying class we use for little-endian access to memory. Is used underneath DrillBufs to abstract away the
 * Netty classes and underlying Netty memory management.
 */
public final class UnsafeDirectLittleEndian extends WrappedByteBuf {
  private static final boolean NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

  public final long id = ID_GENERATOR.incrementAndGet();
  private final AbstractByteBuf wrapped;
  private final long memoryAddress;

  private final AtomicLong bufferCount;
  private final AtomicLong bufferSize;
  private final long initCap;

  UnsafeDirectLittleEndian(DuplicatedByteBuf buf) {
    this(buf, true, null, null);
  }

  UnsafeDirectLittleEndian(LargeBuffer buf) {
    this(buf, true, null, null);
  }

  UnsafeDirectLittleEndian(PooledUnsafeDirectByteBuf buf, AtomicLong bufferCount, AtomicLong bufferSize) {
    this(buf, true, bufferCount, bufferSize);

  }

  private UnsafeDirectLittleEndian(AbstractByteBuf buf, boolean fake, AtomicLong bufferCount, AtomicLong bufferSize) {
    super(buf);
    if (!NATIVE_ORDER || buf.order() != ByteOrder.BIG_ENDIAN) {
      throw new IllegalStateException("Drill only runs on LittleEndian systems.");
    }

    this.bufferCount = bufferCount;
    this.bufferSize = bufferSize;

    // initCap is used if we're tracking memory release. If we're in non-debug mode, we'll skip this.
    this.initCap = ASSERT_ENABLED ? buf.capacity() : -1;

    this.wrapped = buf;
    this.memoryAddress = buf.memoryAddress();
  }
    private long addr(int index) {
        return memoryAddress + index;
    }

    @Override
    public long getLong(int index) {
//        wrapped.checkIndex(index, 8);
        long v = PlatformDependent.getLong(addr(index));
        return v;
    }

    @Override
    public float getFloat(int index) {
        return Float.intBitsToFloat(getInt(index));
    }

  @Override
  public ByteBuf slice() {
    return slice(this.readerIndex(), readableBytes());
  }

  @Override
  public ByteBuf slice(int index, int length) {
    return new SlicedByteBuf(this, index, length);
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
  public double getDouble(int index) {
    return Double.longBitsToDouble(getLong(index));
  }

  @Override
  public char getChar(int index) {
    return (char) getShort(index);
  }

  @Override
  public long getUnsignedInt(int index) {
    return getInt(index) & 0xFFFFFFFFL;
  }

  @Override
  public int getInt(int index) {
    int v = PlatformDependent.getInt(addr(index));
    return v;
  }

  @Override
  public int getUnsignedShort(int index) {
    return getShort(index) & 0xFFFF;
  }

  @Override
  public short getShort(int index) {
    short v = PlatformDependent.getShort(addr(index));
    return v;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    wrapped.checkIndex(index, 2);
    _setShort(index, value);
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    wrapped.checkIndex(index, 4);
    _setInt(index, value);
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    wrapped.checkIndex(index, 8);
    _setLong(index, value);
    return this;
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    setShort(index, value);
    return this;
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    setInt(index, Float.floatToRawIntBits(value));
    return this;
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    setLong(index, Double.doubleToRawLongBits(value));
    return this;
  }

  @Override
  public ByteBuf writeShort(int value) {
    wrapped.ensureWritable(2);
    _setShort(wrapped.writerIndex, value);
    wrapped.writerIndex += 2;
    return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
    wrapped.ensureWritable(4);
    _setInt(wrapped.writerIndex, value);
    wrapped.writerIndex += 4;
    return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
    wrapped.ensureWritable(8);
    _setLong(wrapped.writerIndex, value);
    wrapped.writerIndex += 8;
    return this;
  }

  @Override
  public ByteBuf writeChar(int value) {
    writeShort(value);
    return this;
  }

  @Override
  public ByteBuf writeFloat(float value) {
    writeInt(Float.floatToRawIntBits(value));
    return this;
  }

  @Override
  public ByteBuf writeDouble(double value) {
    writeLong(Double.doubleToRawLongBits(value));
    return this;
  }

  private void _setShort(int index, int value) {
    PlatformDependent.putShort(addr(index), (short) value);
  }

  private void _setInt(int index, int value) {
    PlatformDependent.putInt(addr(index), value);
  }

  private void _setLong(int index, long value) {
    PlatformDependent.putLong(addr(index), value);
  }

  @Override
  public byte getByte(int index) {
    return PlatformDependent.getByte(addr(index));
  }

  @Override
  public ByteBuf setByte(int index, int value) {
    PlatformDependent.putByte(addr(index), (byte) value);
    return this;
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    final boolean released = super.release(decrement);
    if (ASSERT_ENABLED && released && bufferCount != null && bufferSize != null) {
      bufferCount.decrementAndGet();
      bufferSize.addAndGet(-initCap);
    }
    return released;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  public static final boolean ASSERT_ENABLED;

  static {
    boolean isAssertEnabled = false;
    assert isAssertEnabled = true;
    ASSERT_ENABLED = isAssertEnabled;
  }

}

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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.util.internal.PlatformDependent;

/**
 * The underlying class we use for little-endian access to memory. Is used underneath ArrowBufs to abstract away the
 * Netty classes and underlying Netty memory management.
 */
public class UnsafeDirectLittleEndian extends WrappedByteBuf {
  private static final boolean NATIVE_ORDER = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

  public final long id = ID_GENERATOR.incrementAndGet();
  private final AbstractByteBuf wrapped;
  private final long memoryAddress;

  UnsafeDirectLittleEndian(DuplicatedByteBuf buf) {
    this(buf, true);
  }

  UnsafeDirectLittleEndian(LargeBuffer buf) {
    this(buf, true);
  }

  UnsafeDirectLittleEndian(PooledUnsafeDirectByteBuf buf) {
    this(buf, true);

  }

  private UnsafeDirectLittleEndian(AbstractByteBuf buf, boolean fake) {
    super(buf);
    if (!NATIVE_ORDER || buf.order() != ByteOrder.BIG_ENDIAN) {
      throw new IllegalStateException("Arrow only runs on LittleEndian systems.");
    }

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
  public int setBytes(int index, InputStream in, int length) throws IOException {
    wrapped.checkIndex(index, length);
    byte[] tmp = new byte[length];
    int readBytes = in.read(tmp);
    if (readBytes > 0) {
      PlatformDependent.copyMemory(tmp, 0, addr(index), readBytes);
    }
    return readBytes;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    wrapped.checkIndex(index, length);
    if (length != 0) {
      byte[] tmp = new byte[length];
      PlatformDependent.copyMemory(addr(index), tmp, 0, length);
      out.write(tmp);
    }
    return this;
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

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

package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.ipc.message.FBSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper around a WritableByteChannel that maintains the position as well adding
 * some common serialization utilities.
 */
public class WriteChannel implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteChannel.class);

  private long currentPosition = 0;

  private final WritableByteChannel out;

  public WriteChannel(WritableByteChannel out) {
    this.out = out;
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  public long getCurrentPosition() {
    return currentPosition;
  }

  public long write(byte[] buffer) throws IOException {
    return write(ByteBuffer.wrap(buffer));
  }

  public long writeZeros(int zeroCount) throws IOException {
    return write(new byte[zeroCount]);
  }

  public long align() throws IOException {
    if (currentPosition % 8 != 0) { // align on 8 byte boundaries
      return writeZeros(8 - (int) (currentPosition % 8));
    }
    return 0;
  }

  public long write(ByteBuffer buffer) throws IOException {
    long length = buffer.remaining();
    LOGGER.debug("Writing buffer with size: " + length);
    out.write(buffer);
    currentPosition += length;
    return length;
  }

  public static byte[] intToBytes(int value) {
    byte[] outBuffer = new byte[4];
    outBuffer[3] = (byte) (value >>> 24);
    outBuffer[2] = (byte) (value >>> 16);
    outBuffer[1] = (byte) (value >>> 8);
    outBuffer[0] = (byte) (value >>> 0);
    return outBuffer;
  }

  public long writeIntLittleEndian(int v) throws IOException {
    return write(intToBytes(v));
  }

  public void write(ArrowBuf buffer) throws IOException {
    ByteBuffer nioBuffer = buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes());
    write(nioBuffer);
  }

  public long write(FBSerializable writer, boolean withSizePrefix) throws IOException {
    ByteBuffer buffer = serialize(writer);
    if (withSizePrefix) {
      writeIntLittleEndian(buffer.remaining());
    }
    return write(buffer);
  }

  public static ByteBuffer serialize(FBSerializable writer) {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int root = writer.writeTo(builder);
    builder.finish(root);
    return builder.dataBuffer();
  }
}

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
package org.apache.arrow.vector.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.vector.schema.ArrowBuffer;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.FBSerializable;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.flatbuffers.FlatBufferBuilder;

import io.netty.buffer.ArrowBuf;

public class ArrowWriter implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  private static final byte[] MAGIC = "ARROW1".getBytes();

  private final WritableByteChannel out;

  private final Schema schema;

  private final List<ArrowBlock> recordBatches = new ArrayList<>();

  private long currentPosition = 0;

  private boolean started = false;

  public ArrowWriter(WritableByteChannel out, Schema schema) {
    this.out = out;
    this.schema = schema;
  }

  private void start() throws IOException {
    writeMagic();
  }

  private long write(byte[] buffer) throws IOException {
    return write(ByteBuffer.wrap(buffer));
  }

  private long writeZeros(int zeroCount) throws IOException {
    return write(new byte[zeroCount]);
  }

  private long align() throws IOException {
    if (currentPosition % 8 != 0) { // align on 8 byte boundaries
      return writeZeros(8 - (int)(currentPosition % 8));
    }
    return 0;
  }

  private long write(ByteBuffer buffer) throws IOException {
    long length = buffer.remaining();
    out.write(buffer);
    currentPosition += length;
    return length;
  }

  private static byte[] intToBytes(int value) {
    byte[] outBuffer = new byte[4];
    outBuffer[3] = (byte)(value >>> 24);
    outBuffer[2] = (byte)(value >>> 16);
    outBuffer[1] = (byte)(value >>>  8);
    outBuffer[0] = (byte)(value >>>  0);
    return outBuffer;
  }

  private long writeIntLittleEndian(int v) throws IOException {
    return write(intToBytes(v));
  }

  // TODO: write dictionaries

  public void writeRecordBatch(ArrowRecordBatch recordBatch) throws IOException {
    checkStarted();
    align();
    // write metadata header
    long offset = currentPosition;
    write(recordBatch);
    align();
    // write body
    long bodyOffset = currentPosition;
    List<ArrowBuf> buffers = recordBatch.getBuffers();
    List<ArrowBuffer> buffersLayout = recordBatch.getBuffersLayout();
    if (buffers.size() != buffersLayout.size()) {
      throw new IllegalStateException("the layout does not match: " + buffers.size() + " != " + buffersLayout.size());
    }
    for (int i = 0; i < buffers.size(); i++) {
      ArrowBuf buffer = buffers.get(i);
      ArrowBuffer layout = buffersLayout.get(i);
      long startPosition = bodyOffset + layout.getOffset();
      if (startPosition != currentPosition) {
        writeZeros((int)(startPosition - currentPosition));
      }
      write(buffer);
      if (currentPosition != startPosition + layout.getSize()) {
        throw new IllegalStateException("wrong buffer size: " + currentPosition + " != " + startPosition + layout.getSize());
      }
    }
    int metadataLength = (int)(bodyOffset - offset);
    if (metadataLength <= 0) {
      throw new InvalidArrowFileException("invalid recordBatch");
    }
    long bodyLength = currentPosition - bodyOffset;
    LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d", offset, metadataLength, bodyLength));
    // add metadata to footer
    recordBatches.add(new ArrowBlock(offset, metadataLength, bodyLength));
  }

  private void write(ArrowBuf buffer) throws IOException {
    write(buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes()));
  }

  private void checkStarted() throws IOException {
    if (!started) {
      started = true;
      start();
    }
  }

  public void close() throws IOException {
    try {
      long footerStart = currentPosition;
      writeFooter();
      int footerLength = (int)(currentPosition - footerStart);
      if (footerLength <= 0 ) {
        throw new InvalidArrowFileException("invalid footer");
      }
      writeIntLittleEndian(footerLength);
      LOGGER.debug(String.format("Footer starts at %d, length: %d", footerStart, footerLength));
      writeMagic();
    } finally {
      out.close();
    }
  }

  private void writeMagic() throws IOException {
    write(MAGIC);
    LOGGER.debug(String.format("magic written, now at %d", currentPosition));
  }

  private void writeFooter() throws IOException {
    // TODO: dictionaries
    write(new ArrowFooter(schema, Collections.<ArrowBlock>emptyList(), recordBatches));
  }

  private long write(FBSerializable writer) throws IOException {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int root = writer.writeTo(builder);
    builder.finish(root);
    return write(builder.dataBuffer());
  }

}

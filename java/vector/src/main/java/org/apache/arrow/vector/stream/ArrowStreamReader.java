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
package org.apache.arrow.vector.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.arrow.flatbuf.StreamHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.file.ArrowReadUtil;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * This classes reads from an input stream and produces ArrowRecordBatches.
 */
public class ArrowStreamReader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowStreamReader.class);

  private ReadableByteChannel in;
  private final BufferAllocator allocator;
  private ArrowStreamHeader header;
  private long bytesRead = 0;

  /**
   * Constructs a streaming read, reading bytes from 'in'. Non-blocking.
   */
  public ArrowStreamReader(ReadableByteChannel in, BufferAllocator allocator) {
    super();
    this.in = in;
    this.allocator = allocator;
  }

  public ArrowStreamReader(InputStream in, BufferAllocator allocator)
      throws InvalidArrowStreamException, IOException {
    this(Channels.newChannel(in), allocator);
  }

  /**
   * Initializes the reader. Must be called before the other APIs. This is blocking.
   */
  public void init() throws InvalidArrowStreamException, IOException {
    Preconditions.checkState(this.header == null, "Cannot call init() more than once.");
    this.header = readHeader();
  }

  /**
   * Returns the total number of batches in the stream. Returns -1 if this is unknown.
   */
  public int getTotalBatches() {
    Preconditions.checkState(this.header != null, "Must call init() first.");
    return header.getTotalBatches();
  }

  /**
   * Returns the schema for all records in this stream.
   */
  public Schema getSchema () {
    Preconditions.checkState(this.header != null, "Must call init() first.");
    return header.getSchema();
  }

  public long bytesRead() { return bytesRead; }

  /**
   * Reads and returns the next ArrowRecordBatch. Returns null if this is the end
   * of stream.
   */
  public ArrowRecordBatch nextRecordBatch() throws IOException {
    Preconditions.checkState(this.in != null, "Cannot call after close()");
    Preconditions.checkState(this.header != null, "Must call init() first.");

    ByteBuffer intBuffer = ByteBuffer.allocate(4);
    int numRead = readFully(intBuffer);
    if (numRead == 0) {
      // No more bytes in stream. No more batches.
      return null;
    }
    if (numRead != intBuffer.capacity()) {
      throw new InvalidArrowStreamException(
          "Unexpected end of stream reading batch metadata length.");
    }
    int metadataLength = ArrowReadUtil.bytesToInt(intBuffer.array());
    intBuffer.rewind();
    if (readFully(intBuffer) != intBuffer.capacity()) {
      throw new InvalidArrowStreamException(
          "Unexpected end of stream reading batch body length.");
    }
    int bodyLength = ArrowReadUtil.bytesToInt(intBuffer.array());
    Preconditions.checkState(metadataLength >= 0);
    Preconditions.checkState(bodyLength >= 0);
    int dataLen = metadataLength + bodyLength;

    final ArrowBuf buffer = allocator.buffer(dataLen);
    LOGGER.debug("allocated buffer " + buffer);
    if (readFully(buffer, dataLen) != dataLen) {
      throw new InvalidArrowStreamException(
          "Unexpected end of stream reading batch bytes: " + dataLen);
    }
    return ArrowReadUtil.constructRecordBatch(buffer, metadataLength, bodyLength, false);
  }

  @Override
  public void close() throws IOException {
    if (this.in != null) {
      in.close();
      in = null;
    }
  }

  /**
   * Reads bytes into buffer until it is full (buffer.remaining() == 0). Returns the
   * number of bytes read which can be less than full if there are no more.
   */
  private int readFully(ByteBuffer buffer) throws IOException {
    int totalRead = 0;
    while (buffer.remaining() != 0) {
      int read = in.read(buffer);
      if (read < 0) return totalRead;
      totalRead += read;
      if (read == 0) break;
    }
    this.bytesRead += totalRead;
    return totalRead;
  }

  /**
   * Reads up to len into buffer. Returns bytes read.
   */
  private int readFully(ArrowBuf buffer, int l) throws IOException {
    int n = readFully(buffer.nioBuffer(buffer.writerIndex(), l));
    buffer.writerIndex(n);
    return n;
  }

  /**
   * Reads the header from the beginning of the stream.
   */
  private ArrowStreamHeader readHeader() throws InvalidArrowStreamException, IOException {
    // Read the header size. There is an i32 little endian prefix.
    ByteBuffer buffer = ByteBuffer.allocate(4);
    if (readFully(buffer) != 4) {
      throw new InvalidArrowStreamException("Unexpected end of stream. Invalid header size.");
    }

    int headerLength = ArrowReadUtil.bytesToInt(buffer.array());
    LOGGER.debug(String.format("Reading header with length %d", headerLength));
    buffer = ByteBuffer.allocate(headerLength);
    if (readFully(buffer) != headerLength) {
      throw new InvalidArrowStreamException(
          "Unexpected end of stream trying to read header.");
    }
    buffer.rewind();
    return new ArrowStreamHeader(StreamHeader.getRootAsStreamHeader(buffer));
  }
}

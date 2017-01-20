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
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowReader implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowReader.class);

  public static final byte[] MAGIC = "ARROW1".getBytes();

  private final SeekableByteChannel in;

  private final BufferAllocator allocator;

  private ArrowFooter footer;

  public ArrowReader(SeekableByteChannel in, BufferAllocator allocator) {
    super();
    this.in = in;
    this.allocator = allocator;
  }

  private int readFully(ByteBuffer buffer) throws IOException {
    int total = 0;
    int n;
    do {
      n = in.read(buffer);
      total += n;
    } while (n >= 0 && buffer.remaining() > 0);
    buffer.flip();
    return total;
  }

  public ArrowFooter readFooter() throws IOException {
    if (footer == null) {
      if (in.size() <= (MAGIC.length * 2 + 4)) {
        throw new InvalidArrowFileException("file too small: " + in.size());
      }
      ByteBuffer buffer = ByteBuffer.allocate(4 + MAGIC.length);
      long footerLengthOffset = in.size() - buffer.remaining();
      in.position(footerLengthOffset);
      readFully(buffer);
      byte[] array = buffer.array();
      if (!Arrays.equals(MAGIC, Arrays.copyOfRange(array, 4, array.length))) {
        throw new InvalidArrowFileException("missing Magic number " + Arrays.toString(buffer.array()));
      }
      int footerLength = MessageSerializer.bytesToInt(array);
      if (footerLength <= 0 || footerLength + MAGIC.length * 2 + 4 > in.size()) {
        throw new InvalidArrowFileException("invalid footer length: " + footerLength);
      }
      long footerOffset = footerLengthOffset - footerLength;
      LOGGER.debug(String.format("Footer starts at %d, length: %d", footerOffset, footerLength));
      ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
      in.position(footerOffset);
      readFully(footerBuffer);
      Footer footerFB = Footer.getRootAsFooter(footerBuffer);
      this.footer = new ArrowFooter(footerFB);
    }
    return footer;
  }

  // TODO: read dictionaries

  public ArrowRecordBatch readRecordBatch(ArrowBlock block) throws IOException {
    LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d",
        block.getOffset(), block.getMetadataLength(),
        block.getBodyLength()));
    in.position(block.getOffset());
    ArrowRecordBatch batch =  MessageSerializer.deserializeRecordBatch(
        new ReadChannel(in, block.getOffset()), block, allocator);
    if (batch == null) {
      throw new IOException("Invalid file. No batch at offset: " + block.getOffset());
    }
    return batch;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}

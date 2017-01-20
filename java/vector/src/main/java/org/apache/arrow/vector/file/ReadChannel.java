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
import java.nio.channels.ReadableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ArrowBuf;

public class ReadChannel implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadChannel.class);

  private ReadableByteChannel in;
  private long bytesRead = 0;
  // The starting byte offset into 'in'.
  private final long startByteOffset;

  public ReadChannel(ReadableByteChannel in, long startByteOffset) {
    this.in = in;
    this.startByteOffset = startByteOffset;
  }

  public ReadChannel(ReadableByteChannel in) {
    this(in, 0);
  }

  public long bytesRead() { return bytesRead; }

  /**
   * Reads bytes into buffer until it is full (buffer.remaining() == 0). Returns the
   * number of bytes read which can be less than full if there are no more.
   */
  public int readFully(ByteBuffer buffer) throws IOException {
    LOGGER.debug("Reading buffer with size: " + buffer.remaining());
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
  public int readFully(ArrowBuf buffer, int l) throws IOException {
    int n = readFully(buffer.nioBuffer(buffer.writerIndex(), l));
    buffer.writerIndex(n);
    return n;
  }

  public long getCurrentPositiion() { return startByteOffset + bytesRead; }

  @Override
  public void close() throws IOException {
    if (this.in != null) {
      in.close();
      in = null;
    }
  }
}

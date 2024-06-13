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
package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import org.apache.arrow.memory.ArrowBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Adapter around {@link ReadableByteChannel} that reads into {@linkplain ArrowBuf}s. */
public class ReadChannel implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadChannel.class);

  private ReadableByteChannel in;
  private long bytesRead = 0;

  public ReadChannel(ReadableByteChannel in) {
    this.in = in;
  }

  public long bytesRead() {
    return bytesRead;
  }

  /**
   * Reads bytes into buffer until it is full (buffer.remaining() == 0). Returns the number of bytes
   * read which can be less than full if there are no more.
   *
   * @param buffer The buffer to read to
   * @return the number of byte read
   * @throws IOException if nit enough bytes left to read
   */
  public int readFully(ByteBuffer buffer) throws IOException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Reading buffer with size: {}", buffer.remaining());
    }
    int totalRead = 0;
    while (buffer.remaining() != 0) {
      int read = in.read(buffer);
      if (read == -1) {
        this.bytesRead += totalRead;
        return totalRead;
      }
      totalRead += read;
      if (read == 0) {
        break;
      }
    }
    this.bytesRead += totalRead;
    return totalRead;
  }

  /**
   * Reads up to len into buffer. Returns bytes read.
   *
   * @param buffer the buffer to read to
   * @param length the amount of bytes to read
   * @return the number of bytes read
   * @throws IOException if nit enough bytes left to read
   */
  public long readFully(ArrowBuf buffer, long length) throws IOException {
    boolean fullRead = true;
    long bytesLeft = length;
    while (fullRead && bytesLeft > 0) {
      int bytesToRead = (int) Math.min(bytesLeft, Integer.MAX_VALUE);
      int n = readFully(buffer.nioBuffer(buffer.writerIndex(), bytesToRead));
      buffer.writerIndex(buffer.writerIndex() + n);
      fullRead = n == bytesToRead;
      bytesLeft -= n;
    }
    return length - bytesLeft;
  }

  @Override
  public void close() throws IOException {
    if (this.in != null) {
      in.close();
      in = null;
    }
  }
}

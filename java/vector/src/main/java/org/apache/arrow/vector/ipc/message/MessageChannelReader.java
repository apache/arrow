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

package org.apache.arrow.vector.ipc.message;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;

/**
 * Reads a sequence of messages using a ReadChannel.
 */
public class MessageChannelReader extends MessageReader<ArrowBufReadHolder> {

  protected ReadChannel in;
  protected BufferAllocator allocator;

  /**
   * Construct a MessageReader to read streaming messages from an existing ReadChannel.
   *
   * @param in Channel to read messages from
   * @param allocator BufferAllocator used to read Message body into an ArrowBuf.
   */
  public MessageChannelReader(ReadChannel in, BufferAllocator allocator) {
    this.in = in;
    this.allocator = allocator;
  }

  /**
   * Get the number of bytes read from the ReadChannel.
   *
   * @return number of bytes
   */
  public long bytesRead() {
    return in.bytesRead();
  }

  /**
   * Close the ReadChannel.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    in.close();
  }

  /**
   * Read a Message from the ReadChannel and populate holder or set holder.message to null if
   * no further messages.
   *
   * @param holder Message and message information that is populated when read by implementation.
   * @throws IOException
   */
  @Override
  protected void readMessage(ArrowBufReadHolder holder) throws IOException {
    MessageSerializer.readMessage(holder, in);
  }

  /**
   * When a message is followed by a body of data, read that data into an ArrowBuf. This will
   * only be called when a Message has a body length > 0.
   *
   * @param holder Contains Message and message information, will set holder.bodyBuffer with
   *               message body data read.
   * @throws IOException
   */
  @Override
  protected void readMessageBody(ArrowBufReadHolder holder) throws IOException {
    MessageSerializer.readMessageBody(holder, in, allocator);
  }
}

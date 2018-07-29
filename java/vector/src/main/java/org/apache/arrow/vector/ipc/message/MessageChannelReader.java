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
public class MessageChannelReader implements AutoCloseable {
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
   * Read a Message from the ReadChannel and populate holder if a valid message was read.
   *
   * @param holder Message and message information that is populated when read by implementation
   * @return true if a valid Message was read, false if end-of-stream
   * @throws IOException
   */
  public boolean readNext(MessageHolder holder) throws IOException {

    // Read the flatbuf message and check for end-of-stream
    MessageChannelResult result = MessageSerializer.readMessage(in);
    if (!result.hasMessage()) {
      return false;
    }
    holder.message = result.getMessage();

    // Read message body data if defined in message
    if (result.messageHasBody()) {
      int bodyLength = (int) result.getMessageBodyLength();
      holder.bodyBuffer = MessageSerializer.readMessageBody(in, bodyLength, allocator);
    }

    return true;
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
  @Override
  public void close() throws IOException {
    in.close();
  }
}

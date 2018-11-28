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

package org.apache.arrow.vector.ipc.message;

import java.io.IOException;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;

import io.netty.buffer.ArrowBuf;

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
   * Read a message from the ReadChannel and return a MessageResult containing the Message
   * metadata and optional message body data. Once the end-of-stream has been reached, a null
   * value will be returned. If the message has no body, then MessageResult.getBodyBuffer()
   * returns null.
   *
   * @return MessageResult or null if reached end-of-stream
   * @throws IOException on error
   */
  public MessageResult readNext() throws IOException {

    // Read the flatbuf message and check for end-of-stream
    MessageMetadataResult result = MessageSerializer.readMessage(in);
    if (result == null) {
      return null;
    }
    Message message = result.getMessage();
    ArrowBuf bodyBuffer = null;

    // Read message body data if defined in message
    if (result.messageHasBody()) {
      int bodyLength = (int) result.getMessageBodyLength();
      bodyBuffer = MessageSerializer.readMessageBody(in, bodyLength, allocator);
    }

    return new MessageResult(message, bodyBuffer);
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
   * @throws IOException on error
   */
  @Override
  public void close() throws IOException {
    in.close();
  }
}

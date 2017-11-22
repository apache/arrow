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


import io.netty.buffer.ArrowBuf;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ReadChannel;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads a sequence of messages using a ReadChannel.
 */
public class MessageChannelReader implements MessageReader {

  private ReadChannel in;

  /**
   * Construct from an existing ReadChannel.
   *
   * @param in Channel to read messages from
   */
  public MessageChannelReader(ReadChannel in) {
    this.in = in;
  }

  /**
   * Read the next message from the ReadChannel.
   *
   * @return A Message or null if ReadChannel has no more messages, indicated by message length of 0
   * @throws IOException
   */
  @Override
  public Message readNextMessage() throws IOException {
    // Read the message size. There is an i32 little endian prefix.
    ByteBuffer buffer = ByteBuffer.allocate(4);
    if (in.readFully(buffer) != 4) {
      return null;
    }
    int messageLength = MessageSerializer.bytesToInt(buffer.array());
    if (messageLength == 0) {
      return null;
    }

    buffer = ByteBuffer.allocate(messageLength);
    if (in.readFully(buffer) != messageLength) {
      throw new IOException(
          "Unexpected end of stream trying to read message.");
    }
    buffer.rewind();

    return Message.getRootAsMessage(buffer);
  }

  /**
   * Read a message body from the ReadChannel.
   *
   * @param message Read message that is followed by a body of data
   * @param allocator BufferAllocator to allocate memory for body data
   * @return ArrowBuf containing the message body data
   * @throws IOException
   */
  @Override
  public ArrowBuf readMessageBody(Message message, BufferAllocator allocator) throws IOException {

    int bodyLength = (int) message.bodyLength();

    // Now read the record batch body
    ArrowBuf buffer = allocator.buffer(bodyLength);
    if (in.readFully(buffer, bodyLength) != bodyLength) {
      throw new IOException("Unexpected end of input trying to read batch.");
    }

    return buffer;
  }

  /**
   * Get the number of bytes read from the ReadChannel.
   *
   * @return number of bytes
   */
  @Override
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

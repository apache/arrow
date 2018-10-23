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

import java.nio.ByteBuffer;

import org.apache.arrow.flatbuf.Message;

/**
 * Class to hold resulting Message metadata and buffer containing the serialized Flatbuffer
 * message when reading messages from a ReadChannel. This handles Message metadata only and
 * does not include the message body data, which should be subsequently read into an ArrowBuf.
 */
public class MessageMetadataResult {

  /**
   * Construct a container to hold a deserialized Message metadata, and buffer
   * with the serialized Message as read from a ReadChannel.
   *
   * @param messageLength the length of the serialized Flatbuffer message in bytes
   * @param messageBuffer contains the serialized Flatbuffer Message metadata
   * @param message the deserialized Flatbuffer Message metadata description
   */
  MessageMetadataResult(int messageLength, ByteBuffer messageBuffer, Message message) {
    this.messageLength = messageLength;
    this.messageBuffer = messageBuffer;
    this.message = message;
  }


  /**
   * Get the length of the message metadata in bytes, not including the body length.
   *
   * @return number of bytes in the message metadata buffer.
   */
  public int getMessageLength() {
    return messageLength;
  }

  /**
   * Get the buffer containing the raw message metadata bytes, not including the message body data.
   *
   * @return buffer containing the message metadata
   */
  public ByteBuffer getMessageBuffer() {
    return messageBuffer;
  }

  /**
   * Check if the message is followed by a body. This will be true if the message has a body
   * length > 0, which indicates that a message body needs to be read from the input source.
   *
   * @return true if message has a defined body
   */
  public boolean messageHasBody() {
    return message.bodyLength() > 0;
  }

  /**
   * Get the length of the message body.
   *
   * @return number of bytes of the message body
   */
  public long getMessageBodyLength() {
    return message.bodyLength();
  }

  /**
   * Get the realized flatbuf Message metadata description.
   *
   * @return Message metadata
   */
  public Message getMessage() {
    return message;
  }

  private final int messageLength;
  private final ByteBuffer messageBuffer;
  private final Message message;
}

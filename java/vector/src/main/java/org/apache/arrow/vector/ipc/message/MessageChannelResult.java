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

import java.nio.ByteBuffer;

import org.apache.arrow.flatbuf.Message;

/**
* Class to hold resulting Message and message information when reading messages from a ReadChannel.
*/
public class MessageChannelResult {

  /**
  * Construct a container to hold a message result.
  *
  * @param messageLength the length of the message read in bytes
  * @param messageBuffer contains the raw bytes of the message
  * @param message the realized flatbuf Message
  */
  public MessageChannelResult(int messageLength, ByteBuffer messageBuffer, Message message) {
      this.messageLength = messageLength;
      this.messageBuffer = messageBuffer;
      this.message = message;
    }

  /**
  * Returns status indicating if the MessageResult has a valid message.
  *
  * @return true if the result contains a valid message
  */
  public boolean hasMessage() {
    return message != null;
  }

  /**
  * Get the length of the message in bytes.
  *
  * @return number of bytes in the message buffer.
  */
  public int getMessageLength() {
    return messageLength;
  }

  /**
  * Get the buffer containing the raw message bytes.
  *
  * @return buffer containing the message
  */
  public ByteBuffer getMessageBuffer() {
    return messageBuffer;
  }

  /**
   * Check if the message is valid and is followed by a body.
   *
   * @return true if message has a body
   */
  public boolean messageHasBody() {
    return message != null && message.bodyLength() > 0;
  }

  /**
   * Get the length of the message body.
   *
   * @return number of bytes of the message body
   */
  public long getMessageBodyLength() {
    long bodyLength = 0;
    if (message != null) {
      bodyLength = message.bodyLength();
    }
    return bodyLength;
  }

  /**
  * Get the realized flatbuf Message.
  *
  * @return Message
  */
  public Message getMessage() {
    return message;
  }

  private int messageLength;
  private ByteBuffer messageBuffer;
  private Message message;
}

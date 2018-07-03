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

/**
 * Abstract base class for reading a sequence of messages.
 */
public abstract class MessageReader<T extends MessageReadHolder> {

  /**
   * Read the next message in the sequence and message body, if message has a defined
   * body length.
   *
   * @param holder Data structure to hold message information read.
   * @return true if a Message was read or false if no more messages.
   * @throws IOException
   */
  public boolean readNext(T holder) throws IOException {

    // Read message and body if bodyLength is specified
    readMessage(holder);
    if (holder.message == null) {
      return false;
    }
    if (holder.message.bodyLength() > 0) {
      readMessageBody(holder);
    }
    return true;
  }

  /**
   * Attempt to read a message. If no new message is available, set holder.message to null to
   * indicate no further messages in the sequence.
   *
   * @param holder Message and message information that is populated when read by implementation.
   * @throws IOException
   */
  protected abstract void readMessage(T holder) throws IOException;

  /**
   * Read the body of a message, implementation should populate holder accordingly.
   * Called when holder.message.bodyLength > 0.
   *
   * @param holder Contains message information from readMessage, message body is populated when
   *               read by implementation.
   * @throws IOException
   */
  protected abstract void readMessageBody(T holder) throws IOException;
}

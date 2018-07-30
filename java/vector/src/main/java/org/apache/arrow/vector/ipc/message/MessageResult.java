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

/**
 * Class to hold the Message metadata and body data when reading messages through a
 * MessageChannelReader.
 */
public class MessageResult {

  /**
   * Construct with a valid Message metadata and optional ArrowBuf containing message body
   * data, if any.
   *
   * @param message Deserialized Flatbuffer Message metadata description
   * @param bodyBuffer Optional ArrowBuf containing message body data, null if message has no body
   */
  MessageResult(Message message, ArrowBuf bodyBuffer) {
    this.message = message;
    this.bodyBuffer = bodyBuffer;
  }

  /**
   * Get the Message metadata.
   *
   * @return the Flatbuffer Message metadata
   */
  public Message getMessage() {
    return message;
  }

  /**
   * Get the message body data.
   *
   * @return an ArrowBuf containing the message body data or null if the message has no body
   */
  public ArrowBuf getBodyBuffer() {
    return bodyBuffer;
  }

  private final Message message;
  private final ArrowBuf bodyBuffer;
}

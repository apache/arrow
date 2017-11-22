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

import java.io.IOException;

/**
 * Interface for reading a sequence of messages.
 */
public interface MessageReader {

  /**
   * Read the next message in the sequence.
   *
   * @return The read message or null if reached the end of the message sequence
   * @throws IOException
   */
  Message readNextMessage() throws IOException;

  /**
   * When a message is followed by a body of data, read that data into an ArrowBuf. This should
   * only be called when a Message has a body length > 0.
   *
   * @param message Read message that is followed by a body of data
   * @param allocator BufferAllocator to allocate memory for body data
   * @return An ArrowBuf containing the body of the message that was read
   * @throws IOException
   */
  ArrowBuf readMessageBody(Message message, BufferAllocator allocator) throws IOException;

  /**
   * Return the current number of bytes that have been read.
   *
   * @return number of bytes read
   */
  long bytesRead();

  /**
   * Close any resource opened by the message reader, not including message body allocations.
   *
   * @throws IOException
   */
  void close() throws IOException;
}

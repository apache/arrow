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

package org.apache.arrow.vector.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.file.ArrowReader;
import org.apache.arrow.vector.file.ReadChannel;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This classes reads from an input stream and produces ArrowRecordBatches.
 */
public class ArrowStreamReader extends ArrowReader<ReadChannel> {

  /**
   * Constructs a streaming read, reading bytes from 'in'. Non-blocking.
   *
   * @param in        the stream to read from
   * @param allocator to allocate new buffers
   */
  public ArrowStreamReader(ReadableByteChannel in, BufferAllocator allocator) {
    super(new ReadChannel(in), allocator);
  }

  public ArrowStreamReader(InputStream in, BufferAllocator allocator) {
    this(Channels.newChannel(in), allocator);
  }

  /**
   * Reads the schema message from the beginning of the stream.
   *
   * @param in to allocate new buffers
   * @return the deserialized arrow schema
   */
  @Override
  protected Schema readSchema(ReadChannel in) throws IOException {
    return MessageSerializer.deserializeSchema(in);
  }

  @Override
  protected ArrowMessage readMessage(ReadChannel in, BufferAllocator allocator) throws IOException {
    return MessageSerializer.deserializeMessageBatch(in, allocator);
  }
}

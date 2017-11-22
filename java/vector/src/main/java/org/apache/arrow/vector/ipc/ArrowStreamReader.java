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

package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageReader;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This classes reads from an input stream and produces ArrowRecordBatches.
 */
public class ArrowStreamReader extends ArrowReader {

  private MessageReader messageReader;

  /**
   * Constructs a streaming reader using the MessageReader interface. Non-blocking.
   *
   * @param messageReader interface to get read messages
   * @param allocator to allocate new buffers
   */
  public ArrowStreamReader(MessageReader messageReader, BufferAllocator allocator) {
    super(allocator);
    this.messageReader = messageReader;
  }

  /**
   * Constructs a streaming reader from a ReadableByteChannel input. Non-blocking.
   *
   * @param in ReadableByteChannel to read messages from
   * @param allocator to allocate new buffers
   */
  public ArrowStreamReader(ReadableByteChannel in, BufferAllocator allocator) {
    this(new MessageChannelReader(new ReadChannel(in)), allocator);
  }

  /**
   * Constructs a streaming reader from an InputStream. Non-blocking.
   *
   * @param in InputStream to read messages from
   * @param allocator to allocate new buffers
   */
  public ArrowStreamReader(InputStream in, BufferAllocator allocator) {
    this(Channels.newChannel(in), allocator);
  }

  /**
   * Get the number of bytes read from the stream since constructing the reader.
   *
   * @return number of bytes
   */
  @Override
  public long bytesRead() {
    return messageReader.bytesRead();
  }

  /**
   * Closes the underlying read source.
   *
   * @throws IOException
   */
  @Override
  protected void closeReadSource() throws IOException {
    messageReader.close();
  }

  /**
   * Load the next ArrowRecordBatch to the vector schema root if available.
   *
   * @return true if a batch was read, false on EOS
   * @throws IOException
   */
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();

    Message message = messageReader.readNextMessage();

    // Reached EOS
    if (message == null) {
      return false;
    }

    if (message.headerType() != MessageHeader.RecordBatch) {
      throw new IOException("Expected RecordBatch but header was " + message.headerType());
    }

    ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(messageReader, message, allocator);
    loadRecordBatch(batch);
    return true;
  }

  /**
   * Reads the schema message from the beginning of the stream.
   *
   * @return the deserialized arrow schema
   */
  @Override
  protected Schema readSchema() throws IOException {
    return MessageSerializer.deserializeSchema(messageReader);
  }

  /**
   * Read a dictionary batch message, will be invoked after the schema and before normal record
   * batches are read.
   *
   * @return the deserialized dictionary batch
   * @throws IOException
   */
  @Override
  protected ArrowDictionaryBatch readDictionary() throws IOException {
    Message message = messageReader.readNextMessage();

    if (message.headerType() != MessageHeader.DictionaryBatch) {
      throw new IOException("Expected DictionaryBatch but header was " + message.headerType());
    }

    return MessageSerializer.deserializeDictionaryBatch(messageReader, message, allocator);
  }
}

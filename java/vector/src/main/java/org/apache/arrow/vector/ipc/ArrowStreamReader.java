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

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * This class reads from an input stream and produces ArrowRecordBatches.
 */
public class ArrowStreamReader extends ArrowReader {

  private MessageChannelReader messageReader;

  /**
   * Constructs a streaming reader using a MessageChannelReader. Non-blocking.
   *
   * @param messageReader reader used to get messages from a ReadChannel
   * @param allocator to allocate new buffers
   */
  public ArrowStreamReader(MessageChannelReader messageReader, BufferAllocator allocator) {
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
    this(new MessageChannelReader(new ReadChannel(in), allocator), allocator);
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
    MessageResult result = messageReader.readNext();

    // Reached EOS
    if (result == null) {
      return false;
    }

    if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
      throw new IOException("Expected RecordBatch but header was " + result.getMessage().headerType());
    }

    ArrowBuf bodyBuffer = result.getBodyBuffer();

    // For zero-length batches, need an empty buffer to deserialize the batch
    if (bodyBuffer == null) {
      bodyBuffer = allocator.getEmpty();
    }

    ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
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
    MessageResult result = messageReader.readNext();

    if (result == null) {
      throw new IOException("Unexpected end of input. Missing schema.");
    }

    if (result.getMessage().headerType() != MessageHeader.Schema) {
      throw new IOException("Expected schema but header was " + result.getMessage().headerType());
    }

    return MessageSerializer.deserializeSchema(result.getMessage());
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
    MessageResult result = messageReader.readNext();

    if (result == null) {
      throw new IOException("Unexpected end of input. Expected DictionaryBatch");
    }

    if (result.getMessage().headerType() != MessageHeader.DictionaryBatch) {
      throw new IOException("Expected DictionaryBatch but header was " + result.getMessage().headerType());
    }

    ArrowBuf bodyBuffer = result.getBodyBuffer();

    // For zero-length batches, need an empty buffer to deserialize the batch
    if (bodyBuffer == null) {
      bodyBuffer = allocator.getEmpty();
    }

    return MessageSerializer.deserializeDictionaryBatch(result.getMessage(), bodyBuffer);
  }
}

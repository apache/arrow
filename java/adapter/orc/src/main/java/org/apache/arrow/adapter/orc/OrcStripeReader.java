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

package org.apache.arrow.adapter.orc;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class OrcStripeReader extends ArrowReader {
  private OrcStripeReaderJniWrapper stripeReader;
  private MessageChannelReader schemaReader;

  protected OrcStripeReader(OrcStripeReaderJniWrapper stripeReader, BufferAllocator allocator) {
    super(allocator);
    this.stripeReader = stripeReader;
  }

  /**
   * Load the next ArrowRecordBatch to the vector schema root if available.
   *
   * @return true if a batch was read, false on EOS
   * @throws IOException on error
   */
  @Override
  public boolean loadNextBatch() throws IOException {
    OrcRecordBatch recordBatch = stripeReader.next();
    if (recordBatch == null) {
      return false;
    }

    ArrayList<ArrowBuf> buffers = new ArrayList<>();
    for(OrcMemoryJniWrapper buffer : recordBatch.buffers) {
      buffers.add(new ArrowBuf(
              new OrcReferenceManager(buffer),
              null,
              (int)buffer.getSize(),
              buffer.getMemoryAddress(),
              false));
    }

    loadRecordBatch(new ArrowRecordBatch(
            recordBatch.length,
            recordBatch.nodes.stream()
              .map(buf -> new ArrowFieldNode(buf.getLength(), buf.getNullCount()))
              .collect(Collectors.toList()),
            buffers));
    return true;
  }

  /**
   * Return the number of bytes read from the ReadChannel.
   *
   * @return number of bytes read
   */
  @Override
  public long bytesRead() {
    return 0;
  }

  /**
   * Close the underlying read source.
   *
   * @throws IOException on error
   */
  @Override
  protected void closeReadSource() throws IOException {
    stripeReader.close();
    schemaReader.close();
  }

  /**
   * Read the Schema from the source, will be invoked at the beginning the initialization.
   *
   * @return the read Schema
   * @throws IOException on error
   */
  @Override
  protected Schema readSchema() throws IOException {
    byte[] schemaBytes = stripeReader.getSchema();
    schemaReader = new MessageChannelReader(
                      new ReadChannel(
                        new ByteArrayReadableSeekableByteChannel(schemaBytes)), allocator);

    MessageResult result = schemaReader.readNext();

    if (result == null) {
      throw new IOException("Unexpected end of input. Missing schema.");
    }

    if (result.getMessage().headerType() != MessageHeader.Schema) {
      throw new IOException("Expected schema but header was " + result.getMessage().headerType());
    }

    return MessageSerializer.deserializeSchema(result.getMessage());
  }

  /**
   * Read a dictionary batch from the source, will be invoked after the schema has been read and
   * called N times, where N is the number of dictionaries indicated by the schema Fields.
   *
   * @return the read ArrowDictionaryBatch
   * @throws IOException on error
   */
  @Override
  protected ArrowDictionaryBatch readDictionary() throws IOException {
    MessageResult result = schemaReader.readNext();

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

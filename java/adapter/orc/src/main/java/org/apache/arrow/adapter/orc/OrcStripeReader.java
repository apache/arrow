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

import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;
import org.apache.arrow.vector.ipc.message.MessageResult;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import io.netty.buffer.ArrowBuf;

/**
 * Orc stripe that load data into ArrowRecordBatch.
 */
public class OrcStripeReader extends ArrowReader {
  /**
   * reference to native stripe reader instance.
   */
  private final long nativeInstanceId;

  /**
   * Construct a new instance.
   * @param nativeInstanceId nativeInstanceId of the stripe reader instance, obtained by
   *           calling nextStripeReader from OrcReaderJniWrapper
   * @param allocator memory allocator for accounting.
   */
  OrcStripeReader(long nativeInstanceId, BufferAllocator allocator) {
    super(allocator);
    this.nativeInstanceId = nativeInstanceId;
  }

  @Override
  public boolean loadNextBatch() throws IOException {
    OrcRecordBatch recordBatch = OrcStripeReaderJniWrapper.next(nativeInstanceId);
    if (recordBatch == null) {
      return false;
    }

    ArrayList<ArrowBuf> buffers = new ArrayList<>();
    for (OrcMemoryJniWrapper buffer : recordBatch.buffers) {
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

  @Override
  public long bytesRead() {
    return 0;
  }


  @Override
  protected void closeReadSource() throws IOException {
    OrcStripeReaderJniWrapper.close(nativeInstanceId);
  }

  @Override
  protected Schema readSchema() throws IOException {
    byte[] schemaBytes = OrcStripeReaderJniWrapper.getSchema(nativeInstanceId);

    try (MessageChannelReader schemaReader =
           new MessageChannelReader(
                new ReadChannel(
                new ByteArrayReadableSeekableByteChannel(schemaBytes)), allocator)) {

      MessageResult result = schemaReader.readNext();
      if (result == null) {
        throw new IOException("Unexpected end of input. Missing schema.");
      }

      if (result.getMessage().headerType() != MessageHeader.Schema) {
        throw new IOException("Expected schema but header was " + result.getMessage().headerType());
      }

      return MessageSerializer.deserializeSchema(result.getMessage());
    }
  }

  @Override
  protected ArrowDictionaryBatch readDictionary() throws IOException {
    throw new UnsupportedOperationException();
  }
}

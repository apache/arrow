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
package org.apache.arrow.vector.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArrowFileReader extends ArrowReader<SeekableReadChannel> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFileReader.class);

  private ArrowFooter footer;
  private int currentDictionaryBatch = 0;
  private int currentRecordBatch = 0;

  public ArrowFileReader(SeekableByteChannel in, BufferAllocator allocator) {
    super(new SeekableReadChannel(in), allocator);
  }

  public ArrowFileReader(SeekableReadChannel in, BufferAllocator allocator) {
    super(in, allocator);
  }

  @Override
  protected Schema readSchema(SeekableReadChannel in) throws IOException {
    if (footer == null) {
      if (in.size() <= (ArrowMagic.MAGIC_LENGTH * 2 + 4)) {
        throw new InvalidArrowFileException("file too small: " + in.size());
      }
      ByteBuffer buffer = ByteBuffer.allocate(4 + ArrowMagic.MAGIC_LENGTH);
      long footerLengthOffset = in.size() - buffer.remaining();
      in.setPosition(footerLengthOffset);
      in.readFully(buffer);
      buffer.flip();
      byte[] array = buffer.array();
      if (!ArrowMagic.validateMagic(Arrays.copyOfRange(array, 4, array.length))) {
        throw new InvalidArrowFileException("missing Magic number " + Arrays.toString(buffer.array()));
      }
      int footerLength = MessageSerializer.bytesToInt(array);
      if (footerLength <= 0 || footerLength + ArrowMagic.MAGIC_LENGTH * 2 + 4 > in.size()) {
        throw new InvalidArrowFileException("invalid footer length: " + footerLength);
      }
      long footerOffset = footerLengthOffset - footerLength;
      LOGGER.debug(String.format("Footer starts at %d, length: %d", footerOffset, footerLength));
      ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
      in.setPosition(footerOffset);
      in.readFully(footerBuffer);
      footerBuffer.flip();
      Footer footerFB = Footer.getRootAsFooter(footerBuffer);
      this.footer = new ArrowFooter(footerFB);
    }
    return footer.getSchema();
  }

  @Override
  protected ArrowMessage readMessage(SeekableReadChannel in, BufferAllocator allocator) throws IOException {
    if (currentDictionaryBatch < footer.getDictionaries().size()) {
      ArrowBlock block = footer.getDictionaries().get(currentDictionaryBatch++);
      return readDictionaryBatch(in, block, allocator);
    } else if (currentRecordBatch < footer.getRecordBatches().size()) {
      ArrowBlock block = footer.getRecordBatches().get(currentRecordBatch++);
      return readRecordBatch(in, block, allocator);
    } else {
      return null;
    }
  }

  public List<ArrowBlock> getDictionaryBlocks() throws IOException {
    ensureInitialized();
    return footer.getDictionaries();
  }

  public List<ArrowBlock> getRecordBlocks() throws IOException {
    ensureInitialized();
    return footer.getRecordBatches();
  }

  public void loadRecordBatch(ArrowBlock block) throws IOException {
    ensureInitialized();
    int blockIndex = footer.getRecordBatches().indexOf(block);
    if (blockIndex == -1) {
      throw new IllegalArgumentException("Arrow bock does not exist in record batches: " + block);
    }
    currentRecordBatch = blockIndex;
    loadNextBatch();
  }

  private ArrowDictionaryBatch readDictionaryBatch(SeekableReadChannel in,
                                                   ArrowBlock block,
                                                   BufferAllocator allocator) throws IOException {
    LOGGER.debug(String.format("DictionaryRecordBatch at %d, metadata: %d, body: %d",
       block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
    in.setPosition(block.getOffset());
    ArrowDictionaryBatch batch = MessageSerializer.deserializeDictionaryBatch(in, block, allocator);
    if (batch == null) {
      throw new IOException("Invalid file. No batch at offset: " + block.getOffset());
    }
    return batch;
  }

  private ArrowRecordBatch readRecordBatch(SeekableReadChannel in,
                                           ArrowBlock block,
                                           BufferAllocator allocator) throws IOException {
    LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d",
        block.getOffset(), block.getMetadataLength(),
        block.getBodyLength()));
    in.setPosition(block.getOffset());
    ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(in, block, allocator);
    if (batch == null) {
      throw new IOException("Invalid file. No batch at offset: " + block.getOffset());
    }
    return batch;
  }
}

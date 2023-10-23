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

package org.apache.arrow.vector.ipc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowFooter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link ArrowReader} that reads the standard arrow binary
 * file format.
 */
public class ArrowFileReader extends ArrowReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFileReader.class);

  private SeekableReadChannel in;
  private ArrowFooter footer;
  private int estimatedDictionaryRecordBatch = 0;
  private int currentDictionaryBatch = 0;
  private int currentRecordBatch = 0;

  public ArrowFileReader(
      SeekableReadChannel in, BufferAllocator allocator, CompressionCodec.Factory compressionFactory) {
    super(allocator, compressionFactory);
    this.in = in;
  }

  public ArrowFileReader(
      SeekableByteChannel in, BufferAllocator allocator, CompressionCodec.Factory compressionFactory) {
    this(new SeekableReadChannel(in), allocator, compressionFactory);
  }

  public ArrowFileReader(SeekableReadChannel in, BufferAllocator allocator) {
    this(in, allocator, NoCompressionCodec.Factory.INSTANCE);
  }

  public ArrowFileReader(SeekableByteChannel in, BufferAllocator allocator) {
    this(new SeekableReadChannel(in), allocator);
  }

  @Override
  public long bytesRead() {
    return in.bytesRead();
  }

  @Override
  protected void closeReadSource() throws IOException {
    in.close();
  }

  @Override
  protected Schema readSchema() throws IOException {
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
      if (footerLength <= 0 || footerLength + ArrowMagic.MAGIC_LENGTH * 2 + 4 > in.size() ||
              footerLength > footerLengthOffset) {
        throw new InvalidArrowFileException("invalid footer length: " + footerLength);
      }
      long footerOffset = footerLengthOffset - footerLength;
      LOGGER.debug("Footer starts at {}, length: {}", footerOffset, footerLength);
      ByteBuffer footerBuffer = ByteBuffer.allocate(footerLength);
      in.setPosition(footerOffset);
      in.readFully(footerBuffer);
      footerBuffer.flip();
      Footer footerFB = Footer.getRootAsFooter(footerBuffer);
      this.footer = new ArrowFooter(footerFB);
    }
    MetadataV4UnionChecker.checkRead(footer.getSchema(), footer.getMetadataVersion());
    return footer.getSchema();
  }

  @Override
  public void initialize() throws IOException {
    super.initialize();

    // empty stream, has no dictionaries in IPC.
    if (footer.getRecordBatches().size() == 0) {
      return;
    }
  }

  /**
   * Get custom metadata.
   */
  public Map<String, String> getMetaData() {
    if (footer != null) {
      return footer.getMetaData();
    }
    return new HashMap<>();
  }

  /** Returns true if a batch was read, false if no more batches. */
  @Override
  public boolean loadNextBatch() throws IOException {
    prepareLoadNextBatch();

    if (currentRecordBatch < footer.getRecordBatches().size()) {
      ArrowBlock block = footer.getRecordBatches().get(currentRecordBatch++);
      ArrowRecordBatch batch = readRecordBatch(in, block, allocator);
      loadRecordBatch(batch);
      try {
        loadDictionaries();
      } catch (IOException e) {
        batch.close();
        throw e;
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * Loads any dictionaries that may be needed by the given record batch. It attempts
   * to read as little as possible but may read in more deltas than are necessary for blocks
   * toward the end of the file.
   */
  private void loadDictionaries() throws IOException {
    // initial load
    if (currentDictionaryBatch == 0) {
      for (int i = 0; i < dictionaries.size(); i++) {
        ArrowBlock block = footer.getDictionaries().get(currentDictionaryBatch++);
        ArrowDictionaryBatch dictionaryBatch = readDictionaryBatch(in, block, allocator);
        loadDictionary(dictionaryBatch, false);
      }
      estimatedDictionaryRecordBatch++;
    } else {
      // we need to look for delta dictionaries. It involves a look-ahead, unfortunately.
      HashSet<Long> visited = new HashSet<Long>();
      while (estimatedDictionaryRecordBatch < currentRecordBatch &&
             currentDictionaryBatch < footer.getDictionaries().size()) {
        ArrowBlock block = footer.getDictionaries().get(currentDictionaryBatch++);
        ArrowDictionaryBatch dictionaryBatch = readDictionaryBatch(in, block, allocator);
        long dictionaryId = dictionaryBatch.getDictionaryId();
        if (visited.contains(dictionaryId)) {
          // done
          currentDictionaryBatch--;
          estimatedDictionaryRecordBatch++;
        } else if (!dictionaries.containsKey(dictionaryId)) {
          throw new IOException("Dictionary ID " + dictionaryId + " was written " +
              "after the initial batch. The file does not follow the IPC file protocol.");
        } else if (!dictionaryBatch.isDelta()) {
          throw new IOException("Dictionary ID " + dictionaryId + " was written as a replacement  " +
              "after the initial batch. Replacement dictionaries are not currently allowed in the IPC file protocol.");
        } else {
          loadDictionary(dictionaryBatch, true);
        }
      }
      if (currentDictionaryBatch >= footer.getDictionaries().size()) {
        estimatedDictionaryRecordBatch++;
      }
    }
  }

  public List<ArrowBlock> getDictionaryBlocks() throws IOException {
    ensureInitialized();
    return footer.getDictionaries();
  }

  /**
   * Returns the {@link ArrowBlock} metadata from the file.
   */
  public List<ArrowBlock> getRecordBlocks() throws IOException {
    ensureInitialized();
    return footer.getRecordBatches();
  }

  /**
   * Loads record batch for the given block.
   */
  public boolean loadRecordBatch(ArrowBlock block) throws IOException {
    ensureInitialized();
    int blockIndex = footer.getRecordBatches().indexOf(block);
    if (blockIndex == -1) {
      throw new IllegalArgumentException("Arrow block does not exist in record batches: " + block);
    }
    currentRecordBatch = blockIndex;
    loadDictionaries();
    return loadNextBatch();
  }

  @VisibleForTesting
  ArrowFooter getFooter() {
    return footer;
  }

  private ArrowDictionaryBatch readDictionaryBatch(SeekableReadChannel in,
                                                   ArrowBlock block,
                                                   BufferAllocator allocator) throws IOException {
    LOGGER.debug("DictionaryRecordBatch at {}, metadata: {}, body: {}",
        block.getOffset(), block.getMetadataLength(), block.getBodyLength());
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
    LOGGER.debug("RecordBatch at {}, metadata: {}, body: {}",
        block.getOffset(), block.getMetadataLength(),
        block.getBodyLength());
    in.setPosition(block.getOffset());
    ArrowRecordBatch batch = MessageSerializer.deserializeRecordBatch(in, block, allocator);
    if (batch == null) {
      throw new IOException("Invalid file. No batch at offset: " + block.getOffset());
    }
    return batch;
  }
}

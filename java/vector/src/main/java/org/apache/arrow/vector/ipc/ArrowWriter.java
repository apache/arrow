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
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for implementing Arrow writers for IPC over a WriteChannel.
 */
public abstract class ArrowWriter implements AutoCloseable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  // schema with fields in message format, not memory format
  protected final Schema schema;
  protected final WriteChannel out;

  private final VectorUnloader unloader;
  private final DictionaryProvider dictionaryProvider;
  private final Set<Long> dictionaryIdsUsed = new HashSet<>();

  private final CompressionCodec.Factory compressionFactory;
  private final CompressionUtil.CodecType codecType;
  private final Optional<Integer> compressionLevel;
  private boolean started = false;
  private boolean ended = false;

  private final CompressionCodec codec;

  protected IpcOption option;

  protected ArrowWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    this(root, provider, out, IpcOption.DEFAULT);
  }

  protected ArrowWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out, IpcOption option) {
    this(root, provider, out, option, NoCompressionCodec.Factory.INSTANCE, CompressionUtil.CodecType.NO_COMPRESSION,
            Optional.empty());
  }

  /**
   * Note: fields are not closed when the writer is closed.
   *
   * @param root               the vectors to write to the output
   * @param provider           where to find the dictionaries
   * @param out                the output where to write
   * @param option             IPC write options
   * @param compressionFactory Compression codec factory
   * @param codecType          Compression codec
   * @param compressionLevel   Compression level
   */
  protected ArrowWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out, IpcOption option,
                        CompressionCodec.Factory compressionFactory, CompressionUtil.CodecType codecType,
                        Optional<Integer> compressionLevel) {
    this.out = new WriteChannel(out);
    this.option = option;
    this.dictionaryProvider = provider;

    this.compressionFactory = compressionFactory;
    this.codecType = codecType;
    this.compressionLevel = compressionLevel;
    this.codec = this.compressionLevel.isPresent() ?
            this.compressionFactory.createCodec(this.codecType, this.compressionLevel.get()) :
            this.compressionFactory.createCodec(this.codecType);
    this.unloader = new VectorUnloader(root, /*includeNullCount*/ true, codec,
        /*alignBuffers*/ true);

    List<Field> fields = new ArrayList<>(root.getSchema().getFields().size());

    MetadataV4UnionChecker.checkForUnion(root.getSchema().getFields().iterator(), option.metadataVersion);
    // Convert fields with dictionaries to have dictionary type
    for (Field field : root.getSchema().getFields()) {
      fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIdsUsed));
    }

    this.schema = new Schema(fields, root.getSchema().getCustomMetadata());
  }

  public void start() throws IOException {
    ensureStarted();
  }

  /**
   * Writes the record batch currently loaded in this instance's VectorSchemaRoot.
   */
  public void writeBatch() throws IOException {
    ensureStarted();
    ensureDictionariesWritten(dictionaryProvider, dictionaryIdsUsed);
    try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
      writeRecordBatch(batch);
    }
  }

  protected void writeDictionaryBatch(Dictionary dictionary) throws IOException {
    FieldVector vector = dictionary.getVector();
    long id = dictionary.getEncoding().getId();
    int count = vector.getValueCount();
    VectorSchemaRoot dictRoot = new VectorSchemaRoot(
        Collections.singletonList(vector.getField()),
        Collections.singletonList(vector),
        count);
    VectorUnloader unloader = new VectorUnloader(dictRoot, /*includeNullCount*/ true, this.codec,
        /*alignBuffers*/ true);
    ArrowRecordBatch batch = unloader.getRecordBatch();
    ArrowDictionaryBatch dictionaryBatch = new ArrowDictionaryBatch(id, batch, false);
    try {
      writeDictionaryBatch(dictionaryBatch);
    } finally {
      try {
        dictionaryBatch.close();
      } catch (Exception e) {
        throw new RuntimeException("Error occurred while closing dictionary.", e);
      }
    }
  }

  protected ArrowBlock writeDictionaryBatch(ArrowDictionaryBatch batch) throws IOException {
    ArrowBlock block = MessageSerializer.serialize(out, batch, option);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("DictionaryRecordBatch at {}, metadata: {}, body: {}",
          block.getOffset(), block.getMetadataLength(), block.getBodyLength());
    }
    return block;
  }

  protected ArrowBlock writeRecordBatch(ArrowRecordBatch batch) throws IOException {
    ArrowBlock block = MessageSerializer.serialize(out, batch, option);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("RecordBatch at {}, metadata: {}, body: {}",
          block.getOffset(), block.getMetadataLength(), block.getBodyLength());
    }
    return block;
  }

  public void end() throws IOException {
    ensureStarted();
    ensureEnded();
  }

  public long bytesWritten() {
    return out.getCurrentPosition();
  }

  private void ensureStarted() throws IOException {
    if (!started) {
      started = true;
      startInternal(out);
      // write the schema - for file formats this is duplicated in the footer, but matches
      // the streaming format
      MessageSerializer.serialize(out, schema, option);
    }
  }

  /**
   * Write dictionaries after schema and before recordBatches, dictionaries won't be
   * written if empty stream (only has schema data in IPC).
   */
  protected abstract void ensureDictionariesWritten(DictionaryProvider provider, Set<Long> dictionaryIdsUsed)
      throws IOException;

  private void ensureEnded() throws IOException {
    if (!ended) {
      ended = true;
      endInternal(out);
    }
  }

  protected void startInternal(WriteChannel out) throws IOException {
  }

  protected void endInternal(WriteChannel out) throws IOException {
  }

  @Override
  public void close() {
    try {
      end();
      out.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

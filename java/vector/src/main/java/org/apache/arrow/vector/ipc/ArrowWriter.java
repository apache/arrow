/*
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
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for implementing Arrow writers for IPC over a WriteChannel
 */
public abstract class ArrowWriter implements AutoCloseable {

  protected static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  // schema with fields in message format, not memory format
  protected final Schema schema;
  protected final WriteChannel out;

  private final VectorUnloader unloader;
  private final List<ArrowDictionaryBatch> dictionaries;

  private boolean started = false;
  private boolean ended = false;

  /**
   * Note: fields are not closed when the writer is closed
   *
   * @param root     the vectors to write to the output
   * @param provider where to find the dictionaries
   * @param out      the output where to write
   */
  protected ArrowWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    this.unloader = new VectorUnloader(root);
    this.out = new WriteChannel(out);

    List<Field> fields = new ArrayList<>(root.getSchema().getFields().size());
    Set<Long> dictionaryIdsUsed = new HashSet<>();

    // Convert fields with dictionaries to have dictionary type
    for (Field field : root.getSchema().getFields()) {
      fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIdsUsed));
    }

    // Create a record batch for each dictionary
    this.dictionaries = new ArrayList<>(dictionaryIdsUsed.size());
    for (long id : dictionaryIdsUsed) {
      Dictionary dictionary = provider.lookup(id);
      FieldVector vector = dictionary.getVector();
      int count = vector.getValueCount();
      VectorSchemaRoot dictRoot = new VectorSchemaRoot(
          Collections.singletonList(vector.getField()),
          Collections.singletonList(vector),
          count);
      VectorUnloader unloader = new VectorUnloader(dictRoot);
      ArrowRecordBatch batch = unloader.getRecordBatch();
      this.dictionaries.add(new ArrowDictionaryBatch(id, batch));
    }

    this.schema = new Schema(fields, root.getSchema().getCustomMetadata());
  }

  public void start() throws IOException {
    ensureStarted();
  }

  public void writeBatch() throws IOException {
    ensureStarted();
    try (ArrowRecordBatch batch = unloader.getRecordBatch()) {
      writeRecordBatch(batch);
    }
  }

  protected ArrowBlock writeDictionaryBatch(ArrowDictionaryBatch batch) throws IOException {
    ArrowBlock block = MessageSerializer.serialize(out, batch);
    LOGGER.debug(String.format("DictionaryRecordBatch at %d, metadata: %d, body: %d",
        block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
    return block;
  }

  protected ArrowBlock writeRecordBatch(ArrowRecordBatch batch) throws IOException {
    ArrowBlock block = MessageSerializer.serialize(out, batch);
    LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d",
        block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
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
      MessageSerializer.serialize(out, schema);
      // write out any dictionaries
      for (ArrowDictionaryBatch batch : dictionaries) {
        try {
          writeDictionaryBatch(batch);
        } finally {
          batch.close();
        }
      }
    }
  }

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
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

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
package org.apache.arrow.vector.file;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.stream.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ArrowWriter implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowWriter.class);

  // schema with fields in message format, not memory format
  private final Schema schema;
  private final WriteChannel out;

  private final VectorUnloader unloader;
  private final List<ArrowDictionaryBatch> dictionaries;

  private final List<ArrowBlock> dictionaryBlocks = new ArrayList<>();
  private final List<ArrowBlock> recordBlocks = new ArrayList<>();

  private boolean started = false;
  private boolean ended = false;

  /**
   * Note: fields are not closed when the writer is closed
   *
   * @param root
   * @param provider
   * @param out
   */
  protected ArrowWriter(VectorSchemaRoot root, DictionaryProvider provider, WritableByteChannel out) {
    this.unloader = new VectorUnloader(root);
    this.out = new WriteChannel(out);

    List<Field> fields = new ArrayList<>(root.getSchema().getFields().size());
    Map<Long, ArrowDictionaryBatch> dictionaryBatches = new HashMap<>();

    for (Field field: root.getSchema().getFields()) {
      fields.add(toMessageFormat(field, provider, dictionaryBatches));
    }

    this.schema = new Schema(fields);
    this.dictionaries = Collections.unmodifiableList(new ArrayList<>(dictionaryBatches.values()));
  }

  // in the message format, fields have the dictionary type
  // in the memory format, they have the index type
  private Field toMessageFormat(Field field, DictionaryProvider provider, Map<Long, ArrowDictionaryBatch> batches) {
    DictionaryEncoding encoding = field.getDictionary();
    List<Field> children = field.getChildren();

    if (encoding == null && children.isEmpty()) {
      return field;
    }

    List<Field> updatedChildren = new ArrayList<>(children.size());
    for (Field child: children) {
      updatedChildren.add(toMessageFormat(child, provider, batches));
    }

    ArrowType type;
    if (encoding == null) {
      type = field.getType();
    } else {
      long id = encoding.getId();
      Dictionary dictionary = provider.lookup(id);
      if (dictionary == null) {
        throw new IllegalArgumentException("Could not find dictionary with ID " + id);
      }
      type = dictionary.getVectorType();

      if (!batches.containsKey(id)) {
        FieldVector vector = dictionary.getVector();
        int count = vector.getAccessor().getValueCount();
        VectorSchemaRoot root = new VectorSchemaRoot(ImmutableList.of(field), ImmutableList.of(vector), count);
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch batch = unloader.getRecordBatch();
        batches.put(id, new ArrowDictionaryBatch(id, batch));
      }
    }

    return new Field(field.getName(), field.isNullable(), type, encoding, updatedChildren);
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

  protected void writeRecordBatch(ArrowRecordBatch batch) throws IOException {
    ArrowBlock block = MessageSerializer.serialize(out, batch);
    LOGGER.debug(String.format("RecordBatch at %d, metadata: %d, body: %d",
      block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
    recordBlocks.add(block);
  }

  public void end() throws IOException {
    ensureStarted();
    ensureEnded();
  }

  public long bytesWritten() { return out.getCurrentPosition(); }

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
          ArrowBlock block = MessageSerializer.serialize(out, batch);
          LOGGER.debug(String.format("DictionaryRecordBatch at %d, metadata: %d, body: %d",
            block.getOffset(), block.getMetadataLength(), block.getBodyLength()));
          dictionaryBlocks.add(block);
        } finally {
          batch.close();
        }
      }
    }
  }

  private void ensureEnded() throws IOException {
    if (!ended) {
      ended = true;
      endInternal(out, schema, dictionaryBlocks, recordBlocks);
    }
  }

  protected abstract void startInternal(WriteChannel out) throws IOException;

  protected abstract void endInternal(WriteChannel out,
                                      Schema schema,
                                      List<ArrowBlock> dictionaries,
                                      List<ArrowBlock> records) throws IOException;

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

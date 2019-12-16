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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

/**
 * Abstract class to read Schema and ArrowRecordBatches.
 *
 */
public abstract class ArrowReader implements DictionaryProvider, AutoCloseable {

  protected final BufferAllocator allocator;
  private VectorLoader loader;
  private VectorSchemaRoot root;
  protected Map<Long, Dictionary> dictionaries;
  private boolean initialized = false;

  protected ArrowReader(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Returns the vector schema root. This will be loaded with new values on every call to loadNextBatch.
   *
   * @return the vector schema root
   * @throws IOException if reading of schema fails
   */
  public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
    ensureInitialized();
    return root;
  }

  /**
   * Returns any dictionaries that were loaded along with ArrowRecordBatches.
   *
   * @return Map of dictionaries to dictionary id, empty if no dictionaries loaded
   * @throws IOException if reading of schema fails
   */
  public Map<Long, Dictionary> getDictionaryVectors() throws IOException {
    ensureInitialized();
    return dictionaries;
  }

  /**
   * Lookup a dictionary that has been loaded using the dictionary id.
   *
   * @param id Unique identifier for a dictionary
   * @return the requested dictionary or null if not found
   */
  @Override
  public Dictionary lookup(long id) {
    if (!initialized) {
      throw new IllegalStateException("Unable to lookup until reader has been initialized");
    }

    return dictionaries.get(id);
  }

  /**
   * Load the next ArrowRecordBatch to the vector schema root if available.
   *
   * @return true if a batch was read, false on EOS
   * @throws IOException on error
   */
  public abstract boolean loadNextBatch() throws IOException;

  /**
   * Return the number of bytes read from the ReadChannel.
   *
   * @return number of bytes read
   */
  public abstract long bytesRead();

  /**
   * Close resources, including vector schema root and dictionary vectors, and the
   * underlying read source.
   *
   * @throws IOException on error
   */
  @Override
  public void close() throws IOException {
    close(true);
  }

  /**
   * Close resources, including vector schema root and dictionary vectors. If the flag
   * closeReadChannel is true then close the underlying read source, otherwise leave it open.
   *
   * @param closeReadSource Flag to control if closing the underlying read source
   * @throws IOException on error
   */
  public void close(boolean closeReadSource) throws IOException {
    if (initialized) {
      root.close();
      for (Dictionary dictionary : dictionaries.values()) {
        dictionary.getVector().close();
      }
    }

    if (closeReadSource) {
      closeReadSource();
    }
  }

  /**
   * Close the underlying read source.
   *
   * @throws IOException on error
   */
  protected abstract void closeReadSource() throws IOException;

  /**
   * Read the Schema from the source, will be invoked at the beginning the initialization.
   *
   * @return the read Schema
   * @throws IOException on error
   */
  protected abstract Schema readSchema() throws IOException;

  /**
   * Initialize if not done previously.
   *
   * @throws IOException on error
   */
  protected void ensureInitialized() throws IOException {
    if (!initialized) {
      initialize();
      initialized = true;
    }
  }

  /**
   * Reads the schema and initializes the vectors.
   */
  protected void initialize() throws IOException {
    Schema originalSchema = readSchema();
    List<Field> fields = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    Map<Long, Dictionary> dictionaries = new HashMap<>();

    // Convert fields with dictionaries to have the index type
    for (Field field : originalSchema.getFields()) {
      Field updated = DictionaryUtility.toMemoryFormat(field, allocator, dictionaries);
      fields.add(updated);
      vectors.add(updated.createVector(allocator));
    }
    Schema schema = new Schema(fields, originalSchema.getCustomMetadata());

    this.root = new VectorSchemaRoot(schema, vectors, 0);
    this.loader = new VectorLoader(root);
    this.dictionaries = Collections.unmodifiableMap(dictionaries);
  }

  /**
   * Ensure the reader has been initialized and reset the VectorSchemaRoot row count to 0.
   *
   * @throws IOException on error
   */
  protected void prepareLoadNextBatch() throws IOException {
    ensureInitialized();
    root.setRowCount(0);
  }

  /**
   * Load an ArrowRecordBatch to the readers VectorSchemaRoot.
   *
   * @param batch the record batch to load
   */
  protected void loadRecordBatch(ArrowRecordBatch batch) {
    try {
      loader.load(batch);
    } finally {
      batch.close();
    }
  }

  /**
   * Load an ArrowDictionaryBatch to the readers dictionary vectors.
   *
   * @param dictionaryBatch dictionary batch to load
   */
  protected void loadDictionary(ArrowDictionaryBatch dictionaryBatch) {
    long id = dictionaryBatch.getDictionaryId();
    Dictionary dictionary = dictionaries.get(id);
    if (dictionary == null) {
      throw new IllegalArgumentException("Dictionary ID " + id + " not defined in schema");
    }
    FieldVector vector = dictionary.getVector();
    VectorSchemaRoot root = new VectorSchemaRoot(
        Collections.singletonList(vector.getField()),
        Collections.singletonList(vector), 0);
    VectorLoader loader = new VectorLoader(root);
    try {
      loader.load(dictionaryBatch.getDictionary());
    } finally {
      dictionaryBatch.close();
    }
  }
}

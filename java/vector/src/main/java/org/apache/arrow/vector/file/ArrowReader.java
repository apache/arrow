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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowMessage;
import org.apache.arrow.vector.schema.ArrowMessage.ArrowMessageVisitor;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

/**
 * Abstract class to read ArrowRecordBatches from a ReadChannel.
 *
 * @param <T> Type of ReadChannel to use
 */
public abstract class ArrowReader<T extends ReadChannel> implements DictionaryProvider, AutoCloseable {

  private final T in;
  private final BufferAllocator allocator;

  private VectorLoader loader;
  private VectorSchemaRoot root;
  private Map<Long, Dictionary> dictionaries;

  private boolean initialized = false;

  protected ArrowReader(T in, BufferAllocator allocator) {
    this.in = in;
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
   * @throws IOException
   */
  public boolean loadNextBatch() throws IOException {
    ensureInitialized();
    // read in all dictionary batches, then stop after our first record batch
    ArrowMessageVisitor<Boolean> visitor = new ArrowMessageVisitor<Boolean>() {
      @Override
      public Boolean visit(ArrowDictionaryBatch message) {
        try {
          load(message);
        } finally {
          message.close();
        }
        return true;
      }

      @Override
      public Boolean visit(ArrowRecordBatch message) {
        try {
          loader.load(message);
        } finally {
          message.close();
        }
        return false;
      }
    };
    root.setRowCount(0);
    ArrowMessage message = readMessage(in, allocator);

    boolean readBatch = false;
    while (message != null) {
      if (!message.accepts(visitor)) {
        readBatch = true;
        break;
      }
      // else read a dictionary
      message = readMessage(in, allocator);
    }

    return readBatch;
  }

  /**
   * Return the number of bytes read from the ReadChannel.
   *
   * @return number of bytes read
   */
  public long bytesRead() {
    return in.bytesRead();
  }

  /**
   * Close resources, including vector schema root and dictionary vectors, and the
   * underlying ReadChannel.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    close(true);
  }

  /**
   * Close resources, including vector schema root and dictionary vectors. If the flag
   * closeReadChannel is true then close the underlying ReadChannel, otherwise leave it open.
   *
   * @param closeReadChannel Flag to control if closing the underlying ReadChannel
   * @throws IOException
   */
  public void close(boolean closeReadChannel) throws IOException {
    if (initialized) {
      root.close();
      for (Dictionary dictionary : dictionaries.values()) {
        dictionary.getVector().close();
      }
    }

    if (closeReadChannel) {
      in.close();
    }
  }

  protected abstract Schema readSchema(T in) throws IOException;

  protected abstract ArrowMessage readMessage(T in, BufferAllocator allocator) throws IOException;

  protected void ensureInitialized() throws IOException {
    if (!initialized) {
      initialize();
      initialized = true;
    }
  }

  /**
   * Reads the schema and initializes the vectors
   */
  private void initialize() throws IOException {
    Schema originalSchema = readSchema(in);
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

  private void load(ArrowDictionaryBatch dictionaryBatch) {
    long id = dictionaryBatch.getDictionaryId();
    Dictionary dictionary = dictionaries.get(id);
    if (dictionary == null) {
      throw new IllegalArgumentException("Dictionary ID " + id + " not defined in schema");
    }
    FieldVector vector = dictionary.getVector();
    VectorSchemaRoot root = new VectorSchemaRoot(ImmutableList.of(vector.getField()), ImmutableList.of(vector), 0);
    VectorLoader loader = new VectorLoader(root);
    loader.load(dictionaryBatch.getDictionary());
  }
}

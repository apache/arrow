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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

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
   * Returns the vector schema root. This will be loaded with new values on every call to loadNextBatch
   *
   * @return the vector schema root
   * @throws IOException if reading of schema fails
   */
  public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
    ensureInitialized();
    return root;
  }

  /**
   * Returns any dictionaries
   *
   * @return dictionaries, if any
   * @throws IOException if reading of schema fails
   */
  public Map<Long, Dictionary> getDictionaryVectors() throws IOException {
    ensureInitialized();
    return dictionaries;
  }

  @Override
  public Dictionary lookup(long id) {
    if (initialized) {
      return dictionaries.get(id);
    } else {
      return null;
    }
  }

  // Returns true if a batch was read, false on EOS
  public boolean loadNextBatch() throws IOException {
    ensureInitialized();
    // read in all dictionary batches, then stop after our first record batch
    ArrowMessageVisitor<Boolean> visitor = new ArrowMessageVisitor<Boolean>() {
      @Override
      public Boolean visit(ArrowDictionaryBatch message) {
        try { load(message); } finally { message.close(); }
        return true;
      }
      @Override
      public Boolean visit(ArrowRecordBatch message) {
        try { loader.load(message); } finally { message.close(); }
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

  public long bytesRead() { return in.bytesRead(); }

  @Override
  public void close() throws IOException {
    if (initialized) {
      root.close();
      for (Dictionary dictionary: dictionaries.values()) {
        dictionary.getVector().close();
      }
    }
    in.close();
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
    Schema schema = readSchema(in);
    List<Field> fields = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    Map<Long, Dictionary> dictionaries = new HashMap<>();

    for (Field field: schema.getFields()) {
      Field updated = toMemoryFormat(field, dictionaries);
      fields.add(updated);
      vectors.add(updated.createVector(allocator));
    }

    this.root = new VectorSchemaRoot(fields, vectors, 0);
    this.loader = new VectorLoader(root);
    this.dictionaries = Collections.unmodifiableMap(dictionaries);
  }

  // in the message format, fields have the dictionary type
  // in the memory format, they have the index type
  private Field toMemoryFormat(Field field, Map<Long, Dictionary> dictionaries) {
    DictionaryEncoding encoding = field.getDictionary();
    List<Field> children = field.getChildren();

    if (encoding == null && children.isEmpty()) {
      return field;
    }

    List<Field> updatedChildren = new ArrayList<>(children.size());
    for (Field child: children) {
      updatedChildren.add(toMemoryFormat(child, dictionaries));
    }

    ArrowType type;
    if (encoding == null) {
      type = field.getType();
    } else {
      // re-type the field for in-memory format
      type = encoding.getIndexType();
      if (type == null) {
        type = new Int(32, true);
      }
      // get existing or create dictionary vector
      if (!dictionaries.containsKey(encoding.getId())) {
        // create a new dictionary vector for the values
        Field dictionaryField = new Field(field.getName(), new FieldType(field.isNullable(), field.getType(), null, null), children);
        FieldVector dictionaryVector = dictionaryField.createVector(allocator);
        dictionaries.put(encoding.getId(), new Dictionary(dictionaryVector, encoding));
      }
    }

    return new Field(field.getName(), new FieldType(field.isNullable(), type, encoding, field.getMetadata()), updatedChildren);
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

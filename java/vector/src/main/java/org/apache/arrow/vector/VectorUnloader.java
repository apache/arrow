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
package org.apache.arrow.vector;

import com.google.common.collect.Lists;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.ArrowVectorType;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class VectorUnloader {

  private Schema schema;
  private final int valueCount;
  private final List<FieldVector> vectors;
  private List<ArrowDictionaryBatch> dictionaryBatches;

  public VectorUnloader(VectorSchemaRoot root) {
    this(root.getSchema(), root.getRowCount(), root.getFieldVectors());
  }

  public VectorUnloader(Schema schema, int valueCount, List<FieldVector> vectors) {
    this.schema = schema; // TODO copy so we don't mutate caller's state?
    this.valueCount = valueCount;
    this.vectors = vectors;
    updateSchemaAndUnloadDictionaries();
  }

  public Schema getSchema() {
    return schema;
  }

  public List<ArrowDictionaryBatch> getDictionaryBatches() { return dictionaryBatches; }

  public ArrowRecordBatch getRecordBatch() {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    for (FieldVector vector : vectors) {
      appendNodes(vector, nodes, buffers);
    }
    return new ArrowRecordBatch(valueCount, nodes, buffers);
  }

  private void appendNodes(FieldVector vector,
                           List<ArrowFieldNode> nodes,
                           List<ArrowBuf> buffers) {
    Accessor accessor = vector.getAccessor();
    nodes.add(new ArrowFieldNode(accessor.getValueCount(), accessor.getNullCount()));
    List<ArrowBuf> fieldBuffers = vector.getFieldBuffers();
    List<ArrowVectorType> expectedBuffers = vector.getField().getTypeLayout().getVectorTypes();
    if (fieldBuffers.size() != expectedBuffers.size()) {
      throw new IllegalArgumentException(String.format(
          "wrong number of buffers for field %s in vector %s. found: %s",
          vector.getField(), vector.getClass().getSimpleName(), fieldBuffers));
    }
    buffers.addAll(fieldBuffers);

    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }

  // translate dictionary fields from in-memory format to message format
  // add dictionary ids, change field types to dictionary type instead of index type
  private void updateSchemaAndUnloadDictionaries() {
    Map<Dictionary, ArrowDictionaryBatch> dictionaries = new HashMap<>();
    Map<Dictionary, Long> dictionaryIds = new HashMap<>();

    // go through once and collect any existing dictionary ids so that we don't duplicate them
    for (FieldVector vector: vectors) {
      if (vector instanceof DictionaryVector) {
        Dictionary dictionary = ((DictionaryVector) vector).getDictionary();
        dictionaryIds.put(dictionary, dictionary.getId());
      }
    }

    // now generate ids for any dictionaries that haven't defined them
    long nextDictionaryId = 0;
    for (Entry<Dictionary, Long> entry: dictionaryIds.entrySet()) {
      if (entry.getValue() == null) {
        while (dictionaryIds.values().contains(nextDictionaryId)) {
          nextDictionaryId++;
        }
        dictionaryIds.put(entry.getKey(), nextDictionaryId);
      }
    }

    // go through again to add dictionary id to the schema fields and to unload the dictionary batches
    for (FieldVector vector: vectors) {
      if (vector instanceof DictionaryVector) {
        Dictionary dictionary = ((DictionaryVector) vector).getDictionary();
        long dictionaryId = dictionaryIds.get(dictionary);
        Field field = vector.getField();
        // find the dictionary field in the schema
        Field schemaField = null;
        int fieldIndex = 0;
        while (fieldIndex < schema.getFields().size()) {
          Field toCheck = schema.getFields().get(fieldIndex);
          if (field.getName().equals(toCheck.getName())) { // TODO more robust comparison?
            schemaField = toCheck;
            break;
          }
          fieldIndex++;
        }
        if (schemaField == null) {
          throw new IllegalArgumentException("Dictionary field " + field + " not found in schema " + schema);
        }

        // update the schema field with the dictionary type and the dictionary id for the message format
        ArrowType dictionaryType = dictionary.getVector().getField().getType();
        Field replacement = new Field(field.getName(), field.isNullable(), dictionaryType, dictionaryId, field.getChildren());
        List<Field> updatedFields = new ArrayList<>(schema.getFields());
        updatedFields.remove(fieldIndex);
        updatedFields.add(fieldIndex, replacement);
        schema = new Schema(updatedFields);

        // unload the dictionary if we haven't already
        if (!dictionaries.containsKey(dictionary)) {
          FieldVector dictionaryVector = dictionary.getVector();
          int valueCount = dictionaryVector.getAccessor().getValueCount();
          List<FieldVector> dictionaryVectors = new ArrayList<>(1);
          dictionaryVectors.add(dictionaryVector);
          Schema dictionarySchema = new Schema(Lists.newArrayList(field));
          VectorUnloader dictionaryUnloader = new VectorUnloader(dictionarySchema, valueCount, dictionaryVectors);
          ArrowRecordBatch dictionaryBatch = dictionaryUnloader.getRecordBatch();
          dictionaries.put(dictionary, new ArrowDictionaryBatch(dictionaryId, dictionaryBatch));
        }
      }
    }
    dictionaryBatches = Collections.unmodifiableList(new ArrayList<>(dictionaries.values()));
  }
}

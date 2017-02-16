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

import com.google.common.collect.Iterators;
import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.complex.DictionaryVector;
import org.apache.arrow.vector.schema.ArrowDictionaryBatch;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.VectorLayout;
import org.apache.arrow.vector.types.Dictionary;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Loads buffers into vectors
 */
public class VectorLoader implements AutoCloseable {

  private final VectorSchemaRoot root;
  private final Map<Long, FieldVector> dictionaryVectors = new HashMap<>();

  /**
   * Creates a vector loader
   *
   * @param schema schema
   * @param allocator buffer allocator
   */
  public VectorLoader(Schema schema, BufferAllocator allocator) {
    List<Field> fields = new ArrayList<>();
    List<FieldVector> vectors = new ArrayList<>();
    // in the message format, fields have dictionary ids and the dictionary type
    // in the memory format, they have no dictionary id and the index type
    for (Field field: schema.getFields()) {
      Long dictionaryId = field.getDictionary();
      if (dictionaryId == null) {
        MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        FieldVector vector = minorType.getNewVector(field.getName(), allocator, null);
        vector.initializeChildrenFromFields(field.getChildren());
        fields.add(field);
        vectors.add(vector);
      } else {
        // create dictionary vector
        // TODO check if already created
        MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
        FieldVector dictionaryVector = minorType.getNewVector(field.getName(), allocator, null);
        dictionaryVector.initializeChildrenFromFields(field.getChildren());
        dictionaryVectors.put(dictionaryId, dictionaryVector);

        // create index vector
        ArrowType dictionaryType = new ArrowType.Int(32, true); // TODO check actual index type
        Field updated = new Field(field.getName(), field.isNullable(), dictionaryType, null);
        minorType = Types.getMinorTypeForArrowType(dictionaryType);
        FieldVector vector = minorType.getNewVector(field.getName(), allocator, null);
        // vector.initializeChildrenFromFields(null);
        DictionaryVector dictionary = new DictionaryVector(vector, new Dictionary(dictionaryVector, dictionaryId, false)); // TODO ordered
        fields.add(updated);
        vectors.add(dictionary);
      }
    }
    this.root = new VectorSchemaRoot(fields, vectors);
  }

  public VectorSchemaRoot getVectorSchemaRoot() { return root; }

  public void load(ArrowDictionaryBatch dictionaryBatch) {
    long id = dictionaryBatch.getDictionaryId();
    FieldVector vector = dictionaryVectors.get(id);
    if (vector == null) {
      throw new IllegalArgumentException("Dictionary ID " + id + " not defined in schema");
    }
    ArrowRecordBatch recordBatch = dictionaryBatch.getDictionary();
    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    loadBuffers(vector, vector.getField(), buffers, nodes);
  }

  /**
   * Loads the record batch in the vectors
   * will not close the record batch
   * @param recordBatch
   */
  public void load(ArrowRecordBatch recordBatch) {
    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    List<Field> fields = root.getSchema().getFields();
    for (int i = 0; i < fields.size(); ++i) {
      Field field = fields.get(i);
      FieldVector fieldVector = root.getVector(field.getName());
      loadBuffers(fieldVector, field, buffers, nodes);
    }
    root.setRowCount(recordBatch.getLength());
    if (nodes.hasNext() || buffers.hasNext()) {
      throw new IllegalArgumentException("not all nodes and buffers where consumed. nodes: " + Iterators.toString(nodes) + " buffers: " + Iterators.toString(buffers));
    }
  }

  private static void loadBuffers(FieldVector vector,
                                  Field field,
                                  Iterator<ArrowBuf> buffers,
                                  Iterator<ArrowFieldNode> nodes) {
    checkArgument(nodes.hasNext(),
        "no more field nodes for for field " + field + " and vector " + vector);
    ArrowFieldNode fieldNode = nodes.next();
    List<VectorLayout> typeLayout = field.getTypeLayout().getVectors();
    List<ArrowBuf> ownBuffers = new ArrayList<>(typeLayout.size());
    for (int j = 0; j < typeLayout.size(); j++) {
      ownBuffers.add(buffers.next());
    }
    try {
      vector.loadFieldBuffers(fieldNode, ownBuffers);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Could not load buffers for field " +
              field + ". error message: " + e.getMessage(), e);
    }
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      checkArgument(children.size() == childrenFromFields.size(), "should have as many children as in the schema: found " + childrenFromFields.size() + " expected " + children.size());
      for (int i = 0; i < childrenFromFields.size(); i++) {
        Field child = children.get(i);
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes);
      }
    }
  }

  @Override
  public void close() { root.close(); }
}

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

package org.apache.arrow.vector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Holder for a set of vectors to be loaded/unloaded.
 */
public class VectorSchemaRoot implements AutoCloseable {

  private Schema schema;
  private int rowCount;
  private final List<FieldVector> fieldVectors;
  private final Map<String, FieldVector> fieldVectorsMap = new HashMap<>();


  /**
   * Constructs new instance containing each of the vectors.
   */
  public VectorSchemaRoot(Iterable<FieldVector> vectors) {
    this(
        StreamSupport.stream(vectors.spliterator(), false).map(t -> t.getField()).collect(Collectors.toList()),
        StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList()),
        0
        );
  }

  /**
   * Constructs a new instance containing the children of parent but not the parent itself.
   */
  public VectorSchemaRoot(FieldVector parent) {
    this(parent.getField().getChildren(), parent.getChildrenFromFields(), parent.getValueCount());
  }

  /**
   * Constructs a new instance.
   *
   * @param fields The types of each vector.
   * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
   * @param rowCount The number of rows contained.
   */
  public VectorSchemaRoot(List<Field> fields, List<FieldVector> fieldVectors, int rowCount) {
    this(new Schema(fields), fieldVectors, rowCount);
  }

  /**
   * Constructs a new instance.
   *
   * @param schema The schema for the vectors.
   * @param fieldVectors The data vectors.
   * @param rowCount  The number of rows
   */
  public VectorSchemaRoot(Schema schema, List<FieldVector> fieldVectors, int rowCount) {
    if (schema.getFields().size() != fieldVectors.size()) {
      throw new IllegalArgumentException("Fields must match field vectors. Found " +
          fieldVectors.size() + " vectors and " + schema.getFields().size() + " fields");
    }
    this.schema = schema;
    this.rowCount = rowCount;
    this.fieldVectors = fieldVectors;
    for (int i = 0; i < schema.getFields().size(); ++i) {
      Field field = schema.getFields().get(i);
      FieldVector vector = fieldVectors.get(i);
      fieldVectorsMap.put(field.getName(), vector);
    }
  }

  /**
   * Creates a new set of empty vectors corresponding to the given schema.
   */
  public static VectorSchemaRoot create(Schema schema, BufferAllocator allocator) {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (Field field : schema.getFields()) {
      FieldVector vector = field.createVector(allocator);
      fieldVectors.add(vector);
    }
    if (fieldVectors.size() != schema.getFields().size()) {
      throw new IllegalArgumentException("The root vector did not create the right number of children. found " +
          fieldVectors.size() + " expected " + schema.getFields().size());
    }
    return new VectorSchemaRoot(schema, fieldVectors, 0);
  }

  /** Constructs a new instance from vectors. */
  public static VectorSchemaRoot of(FieldVector... vectors) {
    return new VectorSchemaRoot(Arrays.stream(vectors).collect(Collectors.toList()));
  }

  /**
   * Do an adaptive allocation of each vector for memory purposes. Sizes will be based on previously
   * defined initial allocation for each vector (and subsequent size learnings).
   */
  public void allocateNew() {
    for (FieldVector v : fieldVectors) {
      v.allocateNew();
    }
    rowCount = 0;
  }

  /**
   * Release all the memory for each vector held in this root. This DOES NOT remove vectors from the container.
   */
  public void clear() {
    for (FieldVector v : fieldVectors) {
      v.clear();
    }
    rowCount = 0;
  }

  public List<FieldVector> getFieldVectors() {
    return fieldVectors.stream().collect(Collectors.toList());
  }

  public FieldVector getVector(String name) {
    return fieldVectorsMap.get(name);
  }

  public Schema getSchema() {
    return schema;
  }

  public int getRowCount() {
    return rowCount;
  }

  /**
   * Set the row count of all the vectors in this container. Also sets the value
   * count for each root level contained FieldVector.
   * @param rowCount Number of records.
   */
  public void setRowCount(int rowCount) {
    this.rowCount = rowCount;
    for (FieldVector v : getFieldVectors()) {
      v.setValueCount(rowCount);
    }
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(fieldVectors);
    } catch (RuntimeException ex) {
      throw ex;
    } catch (Exception ex) {
      // should never happen since FieldVector.close() doesn't throw IOException
      throw new RuntimeException(ex);
    }
  }

  private void printRow(StringBuilder sb, List<Object> row) {
    boolean first = true;
    for (Object v : row) {
      if (first) {
        first = false;
      } else {
        sb.append("\t");
      }
      sb.append(v);
    }
    sb.append("\n");
  }

  /**
   * Returns a tab separated value of vectors (based on their java object representation).
   */
  public String contentToTSVString() {
    StringBuilder sb = new StringBuilder();
    List<Object> row = new ArrayList<>(schema.getFields().size());
    for (Field field : schema.getFields()) {
      row.add(field.getName());
    }
    printRow(sb, row);
    for (int i = 0; i < rowCount; i++) {
      row.clear();
      for (FieldVector v : fieldVectors) {
        row.add(v.getObject(i));
      }
      printRow(sb, row);
    }
    return sb.toString();
  }

  /**
   * Synchronizes the schema from the current vectors.
   * In some cases, the schema and the actual vector structure may be different.
   * This can be caused by a promoted writer (For details, please see
   * {@link org.apache.arrow.vector.complex.impl.PromotableWriter}).
   * For example, when writing different types of data to a {@link org.apache.arrow.vector.complex.ListVector}
   * may lead to such a case.
   * When this happens, this method should be called to bring the schema and vector structure in a synchronized state.
   * @return true if the schema is updated, false otherwise.
   */
  public boolean syncSchema() {
    List<Field> oldFields = this.schema.getFields();
    List<Field> newFields = this.fieldVectors.stream().map(ValueVector::getField).collect(Collectors.toList());
    if (!oldFields.equals(newFields)) {
      this.schema = new Schema(newFields);
      return true;
    }
    return false;
  }
}

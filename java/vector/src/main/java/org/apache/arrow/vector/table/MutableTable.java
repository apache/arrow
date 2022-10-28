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

package org.apache.arrow.vector.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

/**
 * MutableTable is a mutable tabular data structure for static or interactive use.
 * See {@link VectorSchemaRoot} for batch processing use cases
 *
 * TODO:
 *      Add a concatenate method that takes Table arguments
 *      Add a method that concatenates a VSR to an existing Table
 *      Extend the concatenate method here to immutable Table
 *      Consider adjusting the memory handling of the concatenate methods
 *      Consider removing the constructors that share memory with externally constructed vectors
 */
public class MutableTable extends BaseTable implements AutoCloseable, Iterable<MutableRow> {

  /**
   * Indexes of any rows that have been marked for deletion.
   * TODO: This is a prototype implementation. Replace with bitmap vector
   */
  private final Set<Integer> deletedRows = new HashSet<>();

  /**
   * Constructs new instance containing each of the given vectors.
   */
  public MutableTable(Iterable<FieldVector> vectors) {
    this(StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList()));
  }

  /**
   * Constructs a new instance from vectors.
   */
  public static MutableTable of(FieldVector... vectors) {
    return new MutableTable(Arrays.stream(vectors).collect(Collectors.toList()));
  }

  /**
   * Constructs new instance containing each of the given vectors.
   */
  public MutableTable(List<FieldVector> fieldVectors) {
    this(fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
  }

  /**
   * Constructs a new instance containing the data from the argument. The VectorSchemaRoot
   * is cleared in the process and its rowCount is set to 0. Memory used by the vectors
   * in the VectorSchemaRoot is transferred to the table.
   *
   * @param vsr  The VectorSchemaRoot providing data for this MutableTable
   */
  public MutableTable(VectorSchemaRoot vsr) {
    this(vsr.getFieldVectors(), vsr.getRowCount());
    vsr.clear();
  }

  /**
   * Constructs a new instance.
   *
   * @param fieldVectors The data vectors.
   * @param rowCount     The number of rows
   */
  public MutableTable(List<FieldVector> fieldVectors, int rowCount) {
    super(fieldVectors, rowCount, null);
  }

  /**
   * Constructs a new instance.
   *
   * @param fieldVectors          The data vectors.
   * @param rowCount              The number of rows
   * @param dictionaryProvider    The dictionary provider containing the dictionaries for any encoded column
   */
  public MutableTable(List<FieldVector> fieldVectors, int rowCount, DictionaryProvider dictionaryProvider) {
    super(fieldVectors, rowCount, dictionaryProvider);
  }

  /**
   * Creates a table on a new set of empty vectors corresponding to the given schema.
   */
  public static MutableTable create(Schema schema, BufferAllocator allocator) {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (Field field : schema.getFields()) {
      FieldVector vector = field.createVector(allocator);
      fieldVectors.add(vector);
    }
    if (fieldVectors.size() != schema.getFields().size()) {
      throw new IllegalArgumentException("The root vector did not create the right number of children. found " +
          fieldVectors.size() + " expected " + schema.getFields().size());
    }
    return new MutableTable(fieldVectors, 0);
  }

  /**
   * Returns a new Table made by concatenating a number of VectorSchemaRoots with the same schema.
   *
   * @param roots the VectorSchemaRoots to concatenate
   */
  public static MutableTable concatenate(BufferAllocator allocator, List<VectorSchemaRoot> roots) {
    assert roots.size() > 0;
    Schema firstSchema = roots.get(0).getSchema();
    int totalRowCount = 0;
    for (VectorSchemaRoot root : roots) {
      if (!root.getSchema().equals(firstSchema)) {
        throw new IllegalArgumentException("All tables must have the same schema");
      }
      totalRowCount += root.getRowCount();
    }

    final int finalTotalRowCount = totalRowCount;
    FieldVector[] newVectors = roots.get(0).getFieldVectors().stream()
        .map(vec -> {
          FieldVector newVector = (FieldVector) vec.getTransferPair(allocator).getTo();
          newVector.setInitialCapacity(finalTotalRowCount);
          newVector.allocateNew();
          newVector.setValueCount(finalTotalRowCount);
          return newVector;
        })
        .toArray(FieldVector[]::new);

    int offset = 0;
    for (VectorSchemaRoot root : roots) {
      int rowCount = root.getRowCount();
      for (int i = 0; i < newVectors.length; i++) {
        FieldVector oldVector = root.getVector(i);
        retryCopyFrom(newVectors[i], oldVector, 0, rowCount, offset);
      }
      offset += rowCount;
    }
    return MutableTable.of(newVectors);
  }

  /**
   * Instead of using copyFromSafe, which checks for memory on each write,
   * this tries to copy over the entire vector and retry if it fails.
   *
   * @param newVector the vector to copy to
   * @param oldVector the vector to copy from
   * @param oldStart the starting index in the old vector
   * @param oldEnd the ending index in the old vector
   * @param newStart the starting index in the new vector
   */
  private static void retryCopyFrom(
      ValueVector newVector,
      ValueVector oldVector,
      int oldStart,
      int oldEnd,
      int newStart) {
    while (true) {
      try {
        for (int i = oldStart; i < oldEnd; i++) {
          newVector.copyFrom(i, i - oldStart + newStart, oldVector);
        }
        break;
      } catch (IndexOutOfBoundsException err) {
        newVector.reAlloc();
      }
    }
  }

  /**
   * Do an adaptive allocation of each vector for memory purposes. Sizes will be based on previously
   * defined initial allocation for each vector (and subsequent size learned).
   */
  void allocateNew() {
    for (FieldVector v : fieldVectors) {
      v.allocateNew();
    }
    rowCount = 0;
  }

  /**
   * Returns this table with no deleted rows.
   *
   * @return this table with any deleted rows removed
   */
  public MutableTable compact() {
    mutableRow().compact();
    return this;
  }

  void clearDeletedRows() {
    this.deletedRows.clear();
  }

  /**
   * Release all the memory for each vector held in this table. This DOES NOT remove vectors from the container.
   */
  public void clear() {
    for (FieldVector v : fieldVectors) {
      v.clear();
    }
    clearDeletedRows();
    rowCount = 0;
  }

  /**
   * Returns a new Table created by adding the given vector to the vectors in this Table.
   *
   * @param index  field index
   * @param vector vector to be added.
   * @return out a copy of this Table with vector added
   */
  public MutableTable addVector(int index, FieldVector vector) {
    return new MutableTable(insertVector(index, vector));
  }

  /**
   * Returns a new Table created by removing the selected Vector from this Table.
   *
   * @param index field index
   * @return out a copy of this Table with vector removed
   */
  public MutableTable removeVector(int index) {
    return new MutableTable(extractVector(index));
  }

  public int deletedRowCount() {
    return this.deletedRows.size();
  }

  /**
   * Returns a Table from the data in this table. Memory is transferred to the new table so this mutable table
   * can no longer be used
   *
   * @return a new Table containing the data in this table
   */
  @Override
  public Table toImmutableTable() {
    Table t = new Table(
        fieldVectors.stream().map(v -> {
          TransferPair transferPair = v.getTransferPair(v.getAllocator());
          transferPair.transfer();
          return (FieldVector) transferPair.getTo();
        }).collect(Collectors.toList())
    );
    clear();
    return t;
  }

  @Override
  public MutableTable toMutableTable() {
    return this;
  }

  /**
   * Sets the rowCount for this MutableTable, and the valueCount for each of its vectors to the argument.
   * @param rowCount  the number of rows in the table and in each vector
   */
  public void setRowCount(int rowCount) {
    super.rowCount = rowCount;
    /*
    TODO: Double check that this isn't wanted
    TODO: Should this be public?
    for (FieldVector v : fieldVectors) {
        v.setValueCount(rowCount);
    }
    */
  }

  /**
   * Marks the row at the given index as deleted.
   *
   * @param rowNumber The 0-based index of the row to be deleted
   */
  public void markRowDeleted(int rowNumber) {
    if (rowNumber >= rowCount) {
      return;
    }
    deletedRows.add(rowNumber);
  }

  /**
   * Returns a MutableRow iterator for this MutableTable.
   */
  @Override
  public Iterator<MutableRow> iterator() {

    return new Iterator<MutableRow>() {

      private final MutableRow row = new MutableRow(MutableTable.this);

      @Override
      public MutableRow next() {
        row.next();
        return row;
      }

      @Override
      public boolean hasNext() {
        return row.hasNext();
      }
    };
  }

  /**
   * Returns a cursor-like row allowing the user to read and modify the values in this table.
   *
   * @return a MutableRow providing access to this table
   */
  public MutableRow mutableRow() {
    return new MutableRow(this);
  }

  /**
   * Returns true if the row at the given index has been deleted and false otherwise.
   * If the index is larger than the number of rows, the method returns true.
   *
   * @param rowNumber The 0-based index of the possibly deleted row
   * @return  true if the row at the index was deleted; false otherwise
   */
  @Override
  public boolean isRowDeleted(int rowNumber) {
    if (rowNumber >= rowCount) {
      return true;
    }
    return deletedRows.contains(rowNumber);
  }

  /**
   * Returns the dictionaryProvider associated with this table, if any.
   * @return a DictionaryProvider or null
   */
  public DictionaryProvider getDictionaryProvider() {
    return dictionaryProvider;
  }
}

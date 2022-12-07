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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Table is an immutable tabular data structure.
 *
 * <p>See {@link VectorSchemaRoot} for batch processing use cases
 *
 * <p>
 * This API is EXPERIMENTAL.
 */
public class Table extends BaseTable implements Iterable<Row> {

  /** Constructs new instance containing each of the given vectors. */
  public Table(Iterable<FieldVector> vectors) {
    this(StreamSupport.stream(vectors.spliterator(), false).collect(Collectors.toList()));
  }

  /** Constructs a new instance from vectors. */
  public static Table of(FieldVector... vectors) {
    return new Table(Arrays.stream(vectors).collect(Collectors.toList()));
  }

  /**
   * Constructs a new instance with the number of rows set to the value count of the first
   * FieldVector.
   *
   * <p>All vectors must have the same value count. Although this is not checked, inconsistent
   * counts may lead to exceptions or other undefined behavior later.
   *
   * @param fieldVectors The data vectors (must be equal in size to <code>fields</code>.
   */
  public Table(List<FieldVector> fieldVectors) {
    this(fieldVectors, fieldVectors.size() == 0 ? 0 : fieldVectors.get(0).getValueCount());
  }

  /**
   * Constructs a new instance.
   *
   * @param fieldVectors The data vectors.
   * @param rowCount The number of rows
   */
  public Table(List<FieldVector> fieldVectors, int rowCount) {
    super(fieldVectors, rowCount, null);
  }

  /**
   * Constructs a new instance.
   *
   * @param fieldVectors The data vectors.
   * @param rowCount The number of rows
   * @param provider A dictionary provider. May be null if none of the vectors is dictionary encoded
   */
  public Table(List<FieldVector> fieldVectors, int rowCount, DictionaryProvider provider) {
    super(fieldVectors, rowCount, provider);
  }

  /**
   * Constructs a new instance containing the data from the argument. Vectors are shared between the
   * Table and VectorSchemaRoot. Direct modification of those vectors is unsafe and should be
   * avoided.
   *
   * @param vsr The VectorSchemaRoot providing data for this Table
   */
  public Table(VectorSchemaRoot vsr) {
    this(vsr.getFieldVectors(), vsr.getRowCount());
    vsr.clear();
  }

  /**
   * Returns a deep copy of this table.
   */
  public Table copy() {
    List<FieldVector> vectorCopies = new ArrayList<>();
    for (int i = 0; i < getVectorCount(); i++) {
      vectorCopies.add(getVectorCopy(i));
    }
    DictionaryProvider providerCopy = null;
    if (dictionaryProvider != null) {
      Set<Long> ids = dictionaryProvider.getDictionaryIds();
      Dictionary[] dictionaryCopies = new Dictionary[ids.size()];
      int i = 0;
      for (Long id : ids) {
        Dictionary src = dictionaryProvider.lookup(id);
        FieldVector srcVector = src.getVector();
        FieldVector destVector = srcVector.getField().createVector(srcVector.getAllocator());
        destVector.copyFromSafe(0, srcVector.getValueCount(), srcVector); // TODO: Remove safe copy for perf
        DictionaryEncoding srcEncoding = src.getEncoding();
        Dictionary dest = new Dictionary(destVector,
            new DictionaryEncoding(srcEncoding.getId(), srcEncoding.isOrdered(), srcEncoding.getIndexType()));
        dictionaryCopies[i] = dest;
        i++;
      }
      providerCopy = new DictionaryProvider.MapDictionaryProvider(dictionaryCopies);
    }
    return new Table(vectorCopies, (int) getRowCount(), providerCopy);
  }
  
  /**
   * Returns a new Table created by adding the given vector to the vectors in this Table.
   *
   * @param index field index
   * @param vector vector to be added.
   * @return out a new Table with vector added
   */
  public Table addVector(int index, FieldVector vector) {
    return new Table(insertVector(index, vector));
  }

  /**
   * Returns a new Table created by removing the selected Vector from this Table.
   *
   * @param index field index
   * @return out a new Table with vector removed
   */
  public Table removeVector(int index) {
    return new Table(extractVector(index));
  }

  /**
   * Slice this table from desired index. Memory is NOT transferred from the vectors in this table
   * to new vectors in the target table. This table is unchanged.
   *
   * @param index start position of the slice
   * @return the sliced table
   */
  public Table slice(int index) {
    return slice(index, this.rowCount - index);
  }

  /**
   * Slice this table at desired index and length. Memory is NOT transferred from the vectors in
   * this table to new vectors in the target table. This table is unchanged.
   *
   * @param index start position of the slice
   * @param length length of the slice
   * @return the sliced table
   */
  public Table slice(int index, int length) {
    Preconditions.checkArgument(index >= 0, "expecting non-negative index");
    Preconditions.checkArgument(length >= 0, "expecting non-negative length");
    Preconditions.checkArgument(index + length <= rowCount, "index + length should <= rowCount");

    if (index == 0 && length == rowCount) {
      return this;
    }

    List<FieldVector> sliceVectors =
        fieldVectors.stream()
            .map(
                v -> {
                  TransferPair transferPair = v.getTransferPair(v.getAllocator());
                  transferPair.splitAndTransfer(index, length);
                  return (FieldVector) transferPair.getTo();
                })
            .collect(Collectors.toList());

    return new Table(sliceVectors);
  }

  /** Returns a Row iterator for this Table. */
  @Override
  public Iterator<Row> iterator() {

    return new Iterator<Row>() {

      private final Row row = new Row(Table.this);

      @Override
      public Row next() {
        row.next();
        return row;
      }

      @Override
      public boolean hasNext() {
        return row.hasNext();
      }
    };
  }
}

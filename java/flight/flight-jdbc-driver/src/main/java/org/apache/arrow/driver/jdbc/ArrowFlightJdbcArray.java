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

package org.apache.arrow.driver.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;

import org.apache.arrow.driver.jdbc.accessor.impl.complex.AbstractArrowFlightJdbcListVectorAccessor;
import org.apache.arrow.driver.jdbc.utils.SqlTypes;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.JsonStringArrayList;
import org.apache.arrow.vector.util.TransferPair;

/**
 * Implementation of {@link Array} using an underlying {@link FieldVector}.
 *
 * @see AbstractArrowFlightJdbcListVectorAccessor
 */
public class ArrowFlightJdbcArray implements Array {

  private final FieldVector dataVector;
  private final long startOffset;
  private final long valuesCount;

  /**
   * Instantiate an {@link Array} backed up by given {@link FieldVector}, limited by a start offset and values count.
   *
   * @param dataVector  underlying FieldVector, containing the Array items.
   * @param startOffset offset from FieldVector pointing to this Array's first value.
   * @param valuesCount how many items this Array contains.
   */
  public ArrowFlightJdbcArray(FieldVector dataVector, long startOffset, long valuesCount) {
    this.dataVector = dataVector;
    this.startOffset = startOffset;
    this.valuesCount = valuesCount;
  }

  @Override
  public String getBaseTypeName() {
    final ArrowType arrowType = this.dataVector.getField().getType();
    return SqlTypes.getSqlTypeNameFromArrowType(arrowType);
  }

  @Override
  public int getBaseType() {
    final ArrowType arrowType = this.dataVector.getField().getType();
    return SqlTypes.getSqlTypeIdFromArrowType(arrowType);
  }

  @Override
  public Object getArray() throws SQLException {
    return getArray(null);
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) throws SQLException {
    if (map != null) {
      throw new SQLFeatureNotSupportedException();
    }

    return getArrayNoBoundCheck(this.dataVector, this.startOffset, this.valuesCount);
  }

  @Override
  public Object getArray(long index, int count) throws SQLException {
    return getArray(index, count, null);
  }

  private void checkBoundaries(long index, int count) {
    if (index < 0 || index + count > this.startOffset + this.valuesCount) {
      throw new ArrayIndexOutOfBoundsException();
    }
  }

  private static Object getArrayNoBoundCheck(ValueVector dataVector, long start, long count) {
    Object[] result = new Object[LargeMemoryUtil.checkedCastToInt(count)];
    for (int i = 0; i < count; i++) {
      result[i] = dataVector.getObject(LargeMemoryUtil.checkedCastToInt(start + i));
    }

    return result;
  }

  @Override
  public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
    if (map != null) {
      throw new SQLFeatureNotSupportedException();
    }

    checkBoundaries(index, count);
    return getArrayNoBoundCheck(this.dataVector,
        LargeMemoryUtil.checkedCastToInt(this.startOffset + index), count);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return this.getResultSet(null);
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
    if (map != null) {
      throw new SQLFeatureNotSupportedException();
    }

    return getResultSetNoBoundariesCheck(this.dataVector, this.startOffset, this.valuesCount);
  }

  @Override
  public ResultSet getResultSet(long index, int count) throws SQLException {
    return getResultSet(index, count, null);
  }

  private static ResultSet getResultSetNoBoundariesCheck(ValueVector dataVector, long start,
                                                         long count)
      throws SQLException {
    TransferPair transferPair = dataVector.getTransferPair(dataVector.getAllocator());
    transferPair.splitAndTransfer(LargeMemoryUtil.checkedCastToInt(start),
        LargeMemoryUtil.checkedCastToInt(count));
    FieldVector vectorSlice = (FieldVector) transferPair.getTo();

    VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(vectorSlice);
    return ArrowFlightJdbcVectorSchemaRootResultSet.fromVectorSchemaRoot(vectorSchemaRoot);
  }

  @Override
  public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map)
      throws SQLException {
    if (map != null) {
      throw new SQLFeatureNotSupportedException();
    }

    checkBoundaries(index, count);
    return getResultSetNoBoundariesCheck(this.dataVector,
        LargeMemoryUtil.checkedCastToInt(this.startOffset + index), count);
  }

  @Override
  public void free() {

  }

  @Override
  public String toString() {
    JsonStringArrayList<Object> array = new JsonStringArrayList<>((int) this.valuesCount);

    try {
      array.addAll(Arrays.asList((Object[]) getArray()));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return array.toString();
  }
}

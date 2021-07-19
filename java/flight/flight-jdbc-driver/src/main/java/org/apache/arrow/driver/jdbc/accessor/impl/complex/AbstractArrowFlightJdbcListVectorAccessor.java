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

package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Map;
import java.util.function.IntSupplier;

import org.apache.arrow.driver.jdbc.ArrowFlightResultSet;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.TransferPair;

public abstract class AbstractArrowFlightJdbcListVectorAccessor extends ArrowFlightJdbcAccessor {

  protected AbstractArrowFlightJdbcListVectorAccessor(IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
  }

  @Override
  public Class<?> getObjectClass() {
    return List.class;
  }

  @Override
  public boolean wasNull() {
    return super.wasNull();
  }

  @Override
  public String getString() {
    return super.getString();
  }

  @Override
  public abstract Array getArray();

  static class ArrayImpl implements Array {
    private final FieldVector dataVector;
    private final long start;
    private final long count;

    public ArrayImpl(FieldVector dataVector, long start, long count) {
      this.dataVector = dataVector;
      this.start = start;
      this.count = count;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getBaseType() throws SQLException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getArray() throws SQLException {
      return getArrayNoBoundCheck(this.dataVector, this.start, this.count);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
      if (map != null) {
        throw new SQLFeatureNotSupportedException();
      }
      return this.getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
      checkBoundaries(index, count);
      return getArrayNoBoundCheck(this.dataVector, LargeMemoryUtil.checkedCastToInt(this.start + index), count);
    }

    private void checkBoundaries(long index, int count) {
      if (index < 0 || index + count > this.start + this.count) {
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
      return this.getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
      return getResultSetNoBoundariesCheck(this.dataVector, this.start, this.count);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
      if (map != null) {
        throw new SQLFeatureNotSupportedException();
      }
      return this.getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
      checkBoundaries(index, count);
      return getResultSetNoBoundariesCheck(this.dataVector, LargeMemoryUtil.checkedCastToInt(this.start + index),
          count);
    }

    private static ResultSet getResultSetNoBoundariesCheck(ValueVector dataVector, long start, long count)
        throws SQLException {
      TransferPair transferPair = dataVector.getTransferPair(dataVector.getAllocator());
      transferPair.splitAndTransfer(LargeMemoryUtil.checkedCastToInt(start), LargeMemoryUtil.checkedCastToInt(count));
      FieldVector vectorSlice = (FieldVector) transferPair.getTo();

      VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.of(vectorSlice);
      return ArrowFlightResultSet.fromVectorSchemaRoot(vectorSchemaRoot);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
      if (map != null) {
        throw new SQLFeatureNotSupportedException();
      }
      return this.getResultSet(index, count);
    }

    @Override
    public void free() throws SQLException {

    }
  }
}


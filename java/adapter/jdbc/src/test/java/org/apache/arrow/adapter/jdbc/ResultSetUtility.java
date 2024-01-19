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

package org.apache.arrow.adapter.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public class ResultSetUtility {

  public static ResultSet generateEmptyResultSet() throws SQLException {
    MockDataElement element = new MockDataElement("string_example");
    MockResultSetMetaData.MockColumnMetaData columnMetaData =
            MockResultSetMetaData.MockColumnMetaData.fromDataElement(element, 1);
    ArrayList<MockResultSetMetaData.MockColumnMetaData> cols = new ArrayList<>();
    cols.add(columnMetaData);
    ResultSetMetaData metadata = new MockResultSetMetaData(cols);
    return MockResultSet.builder()
            .setMetaData(metadata)
            .build();
  }

  public static MockResultSet generateBasicResultSet(int rows) throws SQLException {
    MockResultSet.Builder builder = MockResultSet.builder();
    for (int i = 0; i < rows; i++) {
      builder.addDataElement("row number: " + (i + 1)).addDataElement("data").finishRow();
    }
    return builder.build();
  }

  public static class MockResultSet extends ThrowingResultSet {
    private final ArrayList<MockRow> rows;
    private int index = 0;
    private boolean isClosed = false;
    private ResultSetMetaData metadata;
    private boolean wasNull;

    public MockResultSet(ArrayList<MockRow> rows) throws SQLException {
      this(rows, MockResultSetMetaData.fromRows(rows));
    }

    public MockResultSet(ArrayList<MockRow> rows, ResultSetMetaData metadata) {
      this.rows = rows;
      this.metadata = metadata;
      this.wasNull = false;
    }

    public static Builder builder() {
      return new Builder();
    }

    private void throwIfClosed() throws SQLException {
      if (isClosed) {
        throw new SQLException("ResultSet is already closed!");
      }
    }

    private void setWasNull(MockDataElement element) {
      wasNull = element.isNull();
    }

    @Override
    public boolean next() throws SQLException {
      throwIfClosed();
      index++;
      return index <= rows.size();
    }

    @Override
    public void close() throws SQLException {
      throwIfClosed();
      isClosed = true;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
      throwIfClosed();
      return index == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
      return index > rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
      return index == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
      return index == rows.size();
    }

    @Override
    public void beforeFirst() throws SQLException {
      index = 0;
    }

    @Override
    public void afterLast() throws SQLException {
      index = rows.size();
    }

    private MockRow getCurrentRow() throws SQLException {
      throwIfClosed();
      if (index == 0) {
        throw new SQLException("Index is before first element!");
      }
      if (index <= rows.size()) {
        return rows.get(index - 1);
      }
      throw new SQLException("Unable to fetch row at index: " + index);
    }

    private MockDataElement getDataElementAtCol(int idx) throws SQLException {
      MockRow row = getCurrentRow();
      MockDataElement element = row.getDataElementAtIndex(idx - 1);
      setWasNull(element);
      return element;
    }

    @Override
    public String getString(int idx) throws SQLException {
      return getDataElementAtCol(idx).getString();
    }

    @Override
    public boolean getBoolean(int idx) throws SQLException {
      return getDataElementAtCol(idx).getBoolean();
    }

    @Override
    public short getShort(int idx) throws SQLException {
      return getDataElementAtCol(idx).getShort();
    }

    @Override
    public int getInt(int idx) throws SQLException {
      return getDataElementAtCol(idx).getInt();
    }

    @Override
    public long getLong(int idx) throws SQLException {
      return getDataElementAtCol(idx).getLong();
    }

    @Override
    public float getFloat(int idx) throws SQLException {
      return getDataElementAtCol(idx).getFloat();
    }

    @Override
    public double getDouble(int idx) throws SQLException {
      return getDataElementAtCol(idx).getDouble();
    }

    @Override
    public BigDecimal getBigDecimal(int idx) throws SQLException {
      return getDataElementAtCol(idx).getBigDecimal();
    }

    @Override
    public Date getDate(int idx) throws SQLException {
      return getDataElementAtCol(idx).getDate();
    }

    @Override
    public Time getTime(int idx) throws SQLException {
      return getDataElementAtCol(idx).getTime();
    }

    @Override
    public Timestamp getTimestamp(int idx) throws SQLException {
      return getDataElementAtCol(idx).getTimestamp();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      return metadata;
    }

    @Override
    public boolean wasNull() throws SQLException {
      return wasNull;
    }

    public static class Builder {
      private final ArrayList<MockRow> rows;
      private ArrayList<MockDataElement> bufferedElements;
      private ResultSetMetaData metadata;

      Builder() {
        this.rows = new ArrayList<>();
        this.bufferedElements = new ArrayList<>();
      }

      public Builder finishRow() {
        rows.add(new MockRow(this.bufferedElements));
        this.bufferedElements = new ArrayList<>();
        return this;
      }

      public Builder addDataElement(MockDataElement element) {
        this.bufferedElements.add(element);
        return this;
      }

      public Builder addDataElement(String str) {
        return this.addDataElement(new MockDataElement(str));
      }

      public Builder addDataElement(Object val, int sqlType) {
        return this.addDataElement(new MockDataElement(val, sqlType));
      }

      public Builder setMetaData(ResultSetMetaData metaData) {
        this.metadata = metaData;
        return this;
      }

      public MockResultSet build() throws SQLException {
        if (this.metadata == null) {
          return new MockResultSet(this.rows);
        }
        return new MockResultSet(this.rows, this.metadata);
      }
    }
  }

  public static class MockResultSetMetaData extends ThrowingResultSetMetaData {
    private final List<MockColumnMetaData> columns;

    public MockResultSetMetaData(List<MockColumnMetaData> columns) {
      this.columns = columns;
    }

    @Override
    public int getColumnCount() throws SQLException {
      return columns.size();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
      return columns.get(column - 1).getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
      return columns.get(column - 1).getName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
      return columns.get(column - 1).getType();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
      return columns.get(column - 1).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
      return columns.get(column - 1).getScale();
    }

    @Override
    public int isNullable(int column) throws SQLException {
      return columns.get(column - 1).isNullable();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
      return columns.get(column - 1).getDisplaySize();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
      return columns.get(column - 1).getTypeName();
    }

    public static MockResultSetMetaData fromRows(ArrayList<MockRow> rows) throws SQLException {
      // Note: This attempts to dynamically construct ResultSetMetaData from the first row in a given result set.
      // If there are now rows, or the result set contains no columns, this cannot be dynamically generated and
      // an exception will be thrown.
      if (rows.size() == 0) {
        throw new SQLException("Unable to dynamically generate ResultSetMetaData because row count is zero!");
      }
      MockRow firstRow = rows.get(0);
      if (firstRow.dataElements.size() == 0) {
        throw new SQLException("Unable to dynamically generate ResultSetMetaData because column count is zero!");
      }
      ArrayList<MockColumnMetaData> columns = new ArrayList<>();
      for (int i = 0; i < firstRow.dataElements.size(); i++) {
        MockDataElement element = firstRow.getDataElementAtIndex(i);
        columns.add(MockColumnMetaData.fromDataElement(element, i));
      }
      return new MockResultSetMetaData(columns);
    }

    public static class MockColumnMetaData {
      private int index;
      private int sqlType;
      private int precision;
      private int scale;
      private int nullable;
      private String label;
      private String typeName;
      private int displaySize;


      private MockColumnMetaData() {
      }

      private String getLabel() {
        return label;
      }

      private String getName() {
        return getLabel();
      }

      private int getType() {
        return sqlType;
      }

      private int getPrecision() {
        return precision;
      }

      private int getScale() {
        return scale;
      }

      private int isNullable() {
        return nullable;
      }

      private String getTypeName() {
        return typeName;
      }

      private int getDisplaySize() {
        return displaySize;
      }

      public static MockColumnMetaData fromDataElement(MockDataElement element, int i) throws SQLException {
        return MockColumnMetaData.builder()
                .index(i)
                .sqlType(element.getSqlType())
                .precision(element.getPrecision())
                .scale(element.getScale())
                .nullable(element.isNullable())
                .setTypeName("TYPE")
                .setDisplaySize(420)
                .label("col_" + i)
                .build();
      }

      public static Builder builder() {
        return new Builder();
      }

      public static class Builder {
        private MockColumnMetaData columnMetaData = new MockColumnMetaData();

        public Builder index(int index) {
          this.columnMetaData.index = index;
          return this;
        }

        public Builder label(String label) {
          this.columnMetaData.label = label;
          return this;
        }

        public Builder sqlType(int sqlType) {
          this.columnMetaData.sqlType = sqlType;
          return this;
        }

        public Builder precision(int precision) {
          this.columnMetaData.precision = precision;
          return this;
        }

        public Builder scale(int scale) {
          this.columnMetaData.scale = scale;
          return this;
        }

        public Builder nullable(int nullable) {
          this.columnMetaData.nullable = nullable;
          return this;
        }

        public Builder setTypeName(String typeName) {
          this.columnMetaData.typeName = typeName;
          return this;
        }

        public Builder setDisplaySize(int displaySize) {
          this.columnMetaData.displaySize = displaySize;
          return this;
        }

        public MockColumnMetaData build() {
          return this.columnMetaData;
        }
      }

    }

  }

  public static class MockRow {
    private final ArrayList<MockDataElement> dataElements;

    public MockRow(ArrayList<MockDataElement> elements) {
      this.dataElements = elements;
    }

    public MockDataElement getDataElementAtIndex(int idx) throws SQLException {
      if (idx > dataElements.size()) {
        throw new SQLException("Unable to find data element at position: " + idx);
      }
      return dataElements.get(idx);
    }
  }

  public static class MockDataElement {
    private final Object value;
    private final int sqlType;

    public MockDataElement(String val) {
      this(val, Types.VARCHAR);
    }

    public MockDataElement(Object val, int sqlType) {
      this.value = val;
      this.sqlType = sqlType;
    }

    private boolean isNull() {
      return value == null;
    }

    private String getValueAsString() {
      return value.toString();
    }

    private int getPrecision() throws SQLException {
      switch (this.sqlType) {
        case Types.VARCHAR:
          return getValueAsString().length();
        case Types.DECIMAL:
          return getBigDecimal().precision();
        default:
          throw getExceptionToThrow("Unable to determine precision for data type: " + sqlType);
      }
    }

    private int getScale() throws SQLException {
      switch (this.sqlType) {
        case Types.VARCHAR:
          return 0;
        case Types.DECIMAL:
          return getBigDecimal().scale();
        default:
          throw getExceptionToThrow("Unable to determine scale for data type!");
      }
    }

    private int isNullable() throws SQLException {
      switch (this.sqlType) {
        case Types.VARCHAR:
        case Types.DECIMAL:
          return ResultSetMetaData.columnNullable;
        default:
          return ResultSetMetaData.columnNullableUnknown;
      }
    }

    private int getSqlType() throws SQLException {
      return this.sqlType;
    }

    public BigDecimal getBigDecimal() throws SQLException {
      if (value == null) {
        return null;
      }
      try {
        return new BigDecimal(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public String getString() throws SQLException {
      if (value == null) {
        return null;
      }
      return getValueAsString();
    }

    public boolean getBoolean() throws SQLException {
      if (value == null) {
        return false;
      }
      try {
        return (boolean) value;
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public int getInt() throws SQLException {
      if (value == null) {
        return 0;
      }
      try {
        return Integer.parseInt(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public long getLong() throws SQLException {
      if (value == null) {
        return 0L;
      }
      try {
        return Long.parseLong(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public double getDouble() throws SQLException {
      if (value == null) {
        return 0.0;
      }
      try {
        return Double.parseDouble(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public Date getDate() throws SQLException {
      if (value == null) {
        return null;
      }
      try {
        return Date.valueOf(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public Time getTime() throws SQLException {
      if (value == null) {
        return null;
      }
      try {
        return Time.valueOf(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public Timestamp getTimestamp() throws SQLException {
      if (value == null) {
        return null;
      }
      try {
        return Timestamp.valueOf(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public float getFloat() throws SQLException {
      if (value == null) {
        return 0.0f;
      }
      try {
        return Float.parseFloat(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }

    public short getShort() throws SQLException {
      if (value == null) {
        return 0;
      }
      try {
        return Short.parseShort(getValueAsString());
      } catch (Exception ex) {
        throw new SQLException(ex);
      }
    }
  }


  public static class ThrowingResultSet implements ResultSet {

    @Override
    public boolean next() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void close() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean wasNull() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void clearWarnings() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getCursorName() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isFirst() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isLast() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void beforeFirst() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void afterLast() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean first() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean last() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean previous() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getFetchDirection() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getFetchSize() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getType() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getConcurrency() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean rowInserted() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void insertRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void deleteRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void refreshRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Statement getStatement() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getHoldability() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isClosed() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      throw getExceptionToThrow();
    }
  }

  private static SQLException getExceptionToThrow() {
    return getExceptionToThrow("Method is not implemented!");
  }

  private static SQLException getExceptionToThrow(String message) {
    return new SQLException(message);
  }


  public static class ThrowingResultSetMetaData implements ResultSetMetaData {
    @Override
    public int getColumnCount() throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int isNullable(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getScale(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getTableName(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw getExceptionToThrow();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      throw getExceptionToThrow();
    }
  }
}

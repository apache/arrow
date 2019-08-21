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

package org.apache.arrow.adapter.jdbc.h2;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBigIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBitVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBooleanVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDateVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDecimalVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertFloat4VectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertFloat8VectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertSmallIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeStampVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTinyIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarBinaryVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarcharVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getBinaryValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getBooleanValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getCharArray;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getDecimalValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getDoubleValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getFloatValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getIntValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getLongValues;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Map;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.ArrowVectorIterator;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfig;
import org.apache.arrow.adapter.jdbc.JdbcToArrowConfigBuilder;
import org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper;
import org.apache.arrow.adapter.jdbc.JdbcToArrowUtils;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * JUnit Test Class which contains methods to test JDBC to Arrow data conversion functionality with various data types
 * for H2 database using single test data file.
 */
@RunWith(Parameterized.class)
public class JdbcToArrowTest extends AbstractJdbcToArrowTest {

  protected static final String BIGINT = "BIGINT_FIELD5";
  protected static final String BINARY = "BINARY_FIELD12";
  protected static final String BIT = "BIT_FIELD17";
  protected static final String BLOB = "BLOB_FIELD14";
  protected static final String BOOL = "BOOL_FIELD2";
  protected static final String CHAR = "CHAR_FIELD16";
  protected static final String CLOB = "CLOB_FIELD15";
  protected static final String DATE = "DATE_FIELD10";
  protected static final String DECIMAL = "DECIMAL_FIELD6";
  protected static final String DOUBLE = "DOUBLE_FIELD7";
  protected static final String INT = "INT_FIELD1";
  protected static final String REAL = "REAL_FIELD8";
  protected static final String SMALLINT = "SMALLINT_FIELD4";
  protected static final String TIME = "TIME_FIELD9";
  protected static final String TIMESTAMP = "TIMESTAMP_FIELD11";
  protected static final String TINYINT = "TINYINT_FIELD3";
  protected static final String VARCHAR = "VARCHAR_FIELD13";

  private static final String[] testFiles = {"h2/test1_all_datatypes_h2.yml"};

  /**
   * Constructor which populate table object for each test iteration.
   *
   * @param table Table object
   */
  public JdbcToArrowTest(Table table) {
    this.table = table;
  }

  /**
   * Get the test data as a collection of Table objects for each test iteration.
   *
   * @return Collection of Table objects
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   * @throws IOException on error
   */
  @Parameters
  public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
    return Arrays.asList(prepareTestData(testFiles, JdbcToArrowTest.class));
  }

  /**
   * Test Method to test JdbcToArrow Functionality for various H2 DB based datatypes with only one test data file.
   */
  @Test
  public void testJdbcToArroValues() throws SQLException, IOException {
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE),
        Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery())));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        new RootAllocator(Integer.MAX_VALUE)));
    testDataSets(JdbcToArrow.sqlToArrow(conn.createStatement().executeQuery(table.getQuery()),
        Calendar.getInstance()));
    testDataSets(JdbcToArrow.sqlToArrow(
        conn.createStatement().executeQuery(table.getQuery()),
        new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()).build()));
    testDataSets(JdbcToArrow.sqlToArrow(
        conn,
        table.getQuery(),
        new JdbcToArrowConfigBuilder(new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance()).build()));
  }

  @Test
  public void testJdbcSchemaMetadata() throws SQLException {
    JdbcToArrowConfig config = new JdbcToArrowConfigBuilder(new RootAllocator(0), Calendar.getInstance(), true).build();
    ResultSetMetaData rsmd = conn.createStatement().executeQuery(table.getQuery()).getMetaData();
    Schema schema = JdbcToArrowUtils.jdbcToArrowSchema(rsmd, config);
    JdbcToArrowTestHelper.assertFieldMetadataMatchesResultSetMetadata(rsmd, schema);
  }

  /**
   * This method calls the assert methods for various DataSets.
   *
   * @param root VectorSchemaRoot for test
   */
  public void testDataSets(VectorSchemaRoot root) {
    JdbcToArrowTestHelper.assertFieldMetadataIsEmpty(root);

    assertBigIntVectorValues((BigIntVector) root.getVector(BIGINT), table.getRowCount(),
        getLongValues(table.getValues(), BIGINT));

    assertTinyIntVectorValues((TinyIntVector) root.getVector(TINYINT), table.getRowCount(),
        getIntValues(table.getValues(), TINYINT));

    assertSmallIntVectorValues((SmallIntVector) root.getVector(SMALLINT), table.getRowCount(),
        getIntValues(table.getValues(), SMALLINT));

    assertVarBinaryVectorValues((VarBinaryVector) root.getVector(BINARY), table.getRowCount(),
        getBinaryValues(table.getValues(), BINARY));

    assertVarBinaryVectorValues((VarBinaryVector) root.getVector(BLOB), table.getRowCount(),
        getBinaryValues(table.getValues(), BLOB));

    assertVarcharVectorValues((VarCharVector) root.getVector(CLOB), table.getRowCount(),
        getCharArray(table.getValues(), CLOB));

    assertVarcharVectorValues((VarCharVector) root.getVector(VARCHAR), table.getRowCount(),
        getCharArray(table.getValues(), VARCHAR));

    assertVarcharVectorValues((VarCharVector) root.getVector(CHAR), table.getRowCount(),
        getCharArray(table.getValues(), CHAR));

    assertIntVectorValues((IntVector) root.getVector(INT), table.getRowCount(),
        getIntValues(table.getValues(), INT));

    assertBitVectorValues((BitVector) root.getVector(BIT), table.getRowCount(),
        getIntValues(table.getValues(), BIT));

    assertBooleanVectorValues((BitVector) root.getVector(BOOL), table.getRowCount(),
        getBooleanValues(table.getValues(), BOOL));

    assertDateVectorValues((DateMilliVector) root.getVector(DATE), table.getRowCount(),
        getLongValues(table.getValues(), DATE));

    assertTimeVectorValues((TimeMilliVector) root.getVector(TIME), table.getRowCount(),
        getLongValues(table.getValues(), TIME));

    assertTimeStampVectorValues((TimeStampVector) root.getVector(TIMESTAMP), table.getRowCount(),
        getLongValues(table.getValues(), TIMESTAMP));

    assertDecimalVectorValues((DecimalVector) root.getVector(DECIMAL), table.getRowCount(),
        getDecimalValues(table.getValues(), DECIMAL));

    assertFloat8VectorValues((Float8Vector) root.getVector(DOUBLE), table.getRowCount(),
        getDoubleValues(table.getValues(), DOUBLE));

    assertFloat4VectorValues((Float4Vector) root.getVector(REAL), table.getRowCount(),
        getFloatValues(table.getValues(), REAL));
  }


  @Test
  public void runLargeNumberOfRows() throws IOException, SQLException {
    RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    int x = 0;
    final int targetRows = 600000;
    ResultSet rs = new FakeResultSet(targetRows);
    try (ArrowVectorIterator iter = JdbcToArrow.sqlToArrowVectorIterator(rs, allocator)) {
      while (iter.hasNext()) {
        VectorSchemaRoot root = iter.next();
        x += root.getRowCount();
        root.close();
      }
    } finally {
      allocator.close();
    }

    assertEquals(x, targetRows);
  }

  private class FakeResultSet implements ResultSet {

    public int numRows;

    FakeResultSet(int numRows) {
      this.numRows = numRows;
    }

    @Override
    public boolean next() throws SQLException {
      numRows--;
      return numRows >= 0;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public boolean wasNull() throws SQLException {
      return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
      return "test123test123" + numRows;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
      return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
      return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
      return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
      return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
      return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
      return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
      return 0;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
      return new BigDecimal(5);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
      return new byte[0];
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
      throw new UnsupportedOperationException("get column by label not supported");
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
      return false;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
      return null;
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
      return new byte[0];
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
      return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
      return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
      return new ResultSetMetaData() {
        @Override
        public int getColumnCount() throws SQLException {
          return 5;
        }

        @Override
        public boolean isAutoIncrement(int column) throws SQLException {
          return false;
        }

        @Override
        public boolean isCaseSensitive(int column) throws SQLException {
          return false;
        }

        @Override
        public boolean isSearchable(int column) throws SQLException {
          return false;
        }

        @Override
        public boolean isCurrency(int column) throws SQLException {
          return false;
        }

        @Override
        public int isNullable(int column) throws SQLException {
          return 0;
        }

        @Override
        public boolean isSigned(int column) throws SQLException {
          return false;
        }

        @Override
        public int getColumnDisplaySize(int column) throws SQLException {
          return 0;
        }

        @Override
        public String getColumnLabel(int column) throws SQLException {
          return null;
        }

        @Override
        public String getColumnName(int column) throws SQLException {
          return "col_" + column;
        }

        @Override
        public String getSchemaName(int column) throws SQLException {
          return null;
        }

        @Override
        public int getPrecision(int column) throws SQLException {
          return 0;
        }

        @Override
        public int getScale(int column) throws SQLException {
          return 0;
        }

        @Override
        public String getTableName(int column) throws SQLException {
          return null;
        }

        @Override
        public String getCatalogName(int column) throws SQLException {
          return null;
        }

        @Override
        public int getColumnType(int column) throws SQLException {
          switch (column) {
            case 1:
              return Types.VARCHAR;
            case 2:
              return Types.INTEGER;
            case 3:
              return Types.BIGINT;
            case 4:
              return Types.VARCHAR;
            case 5:
              return Types.VARCHAR;
            default:
              throw new IllegalArgumentException("not supported");
          }

        }

        @Override
        public String getColumnTypeName(int column) throws SQLException {
          return null;
        }

        @Override
        public boolean isReadOnly(int column) throws SQLException {
          return false;
        }

        @Override
        public boolean isWritable(int column) throws SQLException {
          return false;
        }

        @Override
        public boolean isDefinitelyWritable(int column) throws SQLException {
          return false;
        }

        @Override
        public String getColumnClassName(int column) throws SQLException {
          return null;
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
          return null;
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
          return false;
        }
      };
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
      return 0;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
      return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
      return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
      return false;
    }

    @Override
    public boolean isLast() throws SQLException {
      return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
      return false;
    }

    @Override
    public boolean last() throws SQLException {
      return false;
    }

    @Override
    public int getRow() throws SQLException {
      return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
      return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
      return false;
    }

    @Override
    public boolean previous() throws SQLException {
      return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
      return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
      return 0;
    }

    @Override
    public int getType() throws SQLException {
      return 0;
    }

    @Override
    public int getConcurrency() throws SQLException {
      return 0;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
      return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
      return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
      return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {

    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {

    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {

    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {

    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {

    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {

    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {

    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {

    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {

    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {

    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {

    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {

    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {

    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {

    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {

    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {

    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {

    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {

    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {

    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {

    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {

    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {

    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {

    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {

    }

    @Override
    public void insertRow() throws SQLException {

    }

    @Override
    public void updateRow() throws SQLException {

    }

    @Override
    public void deleteRow() throws SQLException {

    }

    @Override
    public void refreshRow() throws SQLException {

    }

    @Override
    public void cancelRowUpdates() throws SQLException {

    }

    @Override
    public void moveToInsertRow() throws SQLException {

    }

    @Override
    public void moveToCurrentRow() throws SQLException {

    }

    @Override
    public Statement getStatement() throws SQLException {
      return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
      return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
      return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
      return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
      return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
      return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
      return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
      return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
      return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {

    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {

    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {

    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {

    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
      return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
      return false;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {

    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
      return null;
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
      return null;
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {

    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {

    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {

    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {

    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {

    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
      return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return false;
    }
  }
}

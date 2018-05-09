/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adapter.jdbc.h2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

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

/**
 * 
 * JUnit Test class to test various datatypes converted into Arrow vector for H2 database
 *
 */
@RunWith(Parameterized.class)
public class JdbcToArrowDataTypesTest {
	private Connection conn = null;
	private Table table;
	
    private static final String BIGINT = "big_int";
    private static final String BINARY = "binary";
    private static final String BIT = "bit";
    private static final String BLOB = "blob";
    private static final String BOOL = "bool";
    private static final String CHAR = "char";
    private static final String CLOB = "clob";
    private static final String DATE = "date";
    private static final String DECIMAL = "decimal";
    private static final String DOUBLE = "double";
    private static final String INT = "int";
    private static final String REAL = "real";
    private static final String SMALLINT = "small_int";
    private static final String TIME = "time";
    private static final String TIMESTAMP = "timestamp";
    private static final String TINYINT = "tiny_int";
    private static final String VARCHAR = "varchar";
    
    private static final String[] testFiles = {
        "h2/test1_bigint_h2.yml",
        "h2/test1_binary_h2.yml",
        "h2/test1_bit_h2.yml",
        "h2/test1_blob_h2.yml",
        "h2/test1_bool_h2.yml",
        "h2/test1_char_h2.yml",
        "h2/test1_clob_h2.yml",
        "h2/test1_date_h2.yml",
        "h2/test1_decimal_h2.yml",
        "h2/test1_double_h2.yml",
        "h2/test1_int_h2.yml",
        "h2/test1_real_h2.yml",
        "h2/test1_smallint_h2.yml",
        "h2/test1_time_h2.yml",
        "h2/test1_timestamp_h2.yml",
        "h2/test1_tinyint_h2.yml",
        "h2/test1_varchar_h2.yml"
    };
    
    /**
     * Constructor which populate table object for each test iteration
     * @param table
     */
    public JdbcToArrowDataTypesTest(Table table) {
        this.table = table;
    }
     
    /**
     * This method creates Table object after reading YAML file
     * @param ymlFilePath
     * @return
     * @throws IOException
     */
    private static Table getTable(String ymlFilePath) throws IOException {
        return new ObjectMapper(new YAMLFactory()).readValue(
                JdbcToArrowDataTypesTest.class.getClassLoader().getResourceAsStream(ymlFilePath), Table.class);
    }

    
    /**
     * This method creates Connection object and DB table and also populate data into table for test
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
        String url = "jdbc:h2:mem:JdbcToArrowTest";
        String driver = "org.h2.Driver";
        Class.forName(driver);
        conn = DriverManager.getConnection(url);
        try (Statement stmt = conn.createStatement();) {
            stmt.executeUpdate(table.getCreate());
            for (String insert : table.getData()) {
                stmt.executeUpdate(insert);
            }
        }
    }
    
    /**
     * Clean up method to close connection after test completes
     * @throws SQLException
     */
    @After
    public void destroy() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }
    
  /**
   * This method returns collection of Table object for each test iteration
   * @return
   * @throws SQLException
   * @throws ClassNotFoundException
   * @throws IOException
   */
    @Parameters
    public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
      	Object[][] tableArr = new Object[testFiles.length][];
        int i = 0;
        for (String testFile: testFiles) {
        	tableArr[i++] = new Object[]{getTable(testFile)};
        }
        return Arrays.asList(tableArr);
    }
    
    /**
     * This method tests various datatypes converted into Arrow vector for H2 database
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testDBValues() throws SQLException, IOException {
        try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(),
                new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())) {
            switch (table.getType()) {
                case BIGINT:
                    assertBigIntVectorValues((BigIntVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
                    break;
                case BINARY:case BLOB:
                    assertVarBinaryVectorValues((VarBinaryVector) root.getVector(table.getVector()), table.getValues().length, table.getBinaryValues());
                    break;
                case BIT:
                    assertBitVectorValues((BitVector) root.getVector(table.getVector()), table.getValues().length, table.getIntValues());
                    break;
                case BOOL:
                    assertBooleanVectorValues((BitVector) root.getVector(table.getVector()), table.getValues().length, table.getBoolValues());
                    break;
                case CHAR:case VARCHAR: case CLOB:
                    assertVarcharVectorValues((VarCharVector) root.getVector(table.getVector()), table.getValues().length, table.getCharValues());
                    break;
                case DATE:
                    assertDateVectorValues((DateMilliVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
                    break;
                case TIME:
                    assertTimeVectorValues((TimeMilliVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
                    break;
                case TIMESTAMP:
                    assertTimeStampVectorValues((TimeStampVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
                    break;
                case DECIMAL:
                    assertDecimalVectorValues((DecimalVector) root.getVector(table.getVector()), table.getValues().length, table.getBigDecimalValues());
                    break;
                case DOUBLE:
                    assertFloat8VectorValues((Float8Vector) root.getVector(table.getVector()), table.getValues().length, table.getDoubleValues());
                    break;
                case INT:
                    assertIntVectorValues((IntVector) root.getVector(table.getVector()), table.getValues().length, table.getIntValues());
                    break;
                case SMALLINT:
                    assertSmallIntVectorValues((SmallIntVector) root.getVector(table.getVector()), table.getValues().length, table.getIntValues());
                    break;
                case TINYINT:
                    assertTinyIntVectorValues((TinyIntVector) root.getVector(table.getVector()), table.getValues().length, table.getIntValues());
                    break;
                case REAL:
                    assertFloat4VectorValues((Float4Vector) root.getVector(table.getVector()), table.getValues().length, table.getFloatValues());
                    break;
            }
        }
    }

}


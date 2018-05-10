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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertNullValues;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * 
 * JUnit Test class to test null values stored into Arrow vector for various datatypes for H2 database
 *
 */
@RunWith(Parameterized.class)
public class JdbcToArrowNullTest {

    private Connection conn = null;
    private Table table;

    private static final String NULL = "null";
    private static final String SELECTED_NULL_COLUMN = "selected_null_column";

    private static final String[] testFiles = {
        "h2/test1_all_datatypes_null_h2.yml",
        "h2/test1_selected_datatypes_null_h2.yml"
    };
    
    /**
     * Constructor which populate table object for each test iteration
     * @param table
     */
    public JdbcToArrowNullTest (Table table) {
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
                JdbcToArrowDataTypesTest.class.getClassLoader().getResourceAsStream(ymlFilePath),
                Table.class);
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
     * This method tests null values stored into Arrow vector for various datatypes for H2 database
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testNullValues() throws SQLException, IOException {
    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(),
             new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())) {
    		switch (table.getType()) {
            	case NULL:
            		sqlToArrowTestNullValues(table.getVectors(), root, table.getRowCount());
            		break;
            	case SELECTED_NULL_COLUMN:
            		sqlToArrowTestSelectedNullColumnsValues(table.getVectors(), root, table.getRowCount());
            		break;
    		}
    	}
    }
    
    /**
     * This method assert tests null values in vectors for all the datatypes 
     * @param vectors
     * @param root
     * @param rowCount
     */
    public void sqlToArrowTestNullValues(String[] vectors, VectorSchemaRoot root, int rowCount) {
		assertNullValues((IntVector)root.getVector(vectors[0]), rowCount);
		assertNullValues((BitVector)root.getVector(vectors[1]), rowCount);
		assertNullValues((TinyIntVector)root.getVector(vectors[2]), rowCount);
		assertNullValues((SmallIntVector)root.getVector(vectors[3]), rowCount);
		assertNullValues((BigIntVector)root.getVector(vectors[4]), rowCount);
		assertNullValues((DecimalVector)root.getVector(vectors[5]), rowCount);
		assertNullValues((Float8Vector)root.getVector(vectors[6]), rowCount);
		assertNullValues((Float4Vector)root.getVector(vectors[7]), rowCount);
		assertNullValues((TimeMilliVector)root.getVector(vectors[8]), rowCount);
		assertNullValues((DateMilliVector)root.getVector(vectors[9]), rowCount);
		assertNullValues((TimeStampVector)root.getVector(vectors[10]), rowCount);
		assertNullValues((VarBinaryVector)root.getVector(vectors[11]), rowCount);
		assertNullValues((VarCharVector)root.getVector(vectors[12]), rowCount);
		assertNullValues((VarBinaryVector)root.getVector(vectors[13]), rowCount);
		assertNullValues((VarCharVector)root.getVector(vectors[14]), rowCount);
		assertNullValues((VarCharVector)root.getVector(vectors[15]), rowCount);
		assertNullValues((BitVector)root.getVector(vectors[16]), rowCount);
    }
    
    /**
     * This method assert tests null values in vectors for some selected datatypes 
     * @param vectors
     * @param root
     * @param rowCount
     */
    public void sqlToArrowTestSelectedNullColumnsValues(String[] vectors, VectorSchemaRoot root, int rowCount) {
		assertNullValues((BigIntVector)root.getVector(vectors[0]), rowCount);
		assertNullValues((DecimalVector)root.getVector(vectors[1]), rowCount);
		assertNullValues((Float8Vector)root.getVector(vectors[2]), rowCount);
		assertNullValues((Float4Vector)root.getVector(vectors[3]), rowCount);
		assertNullValues((TimeMilliVector)root.getVector(vectors[4]), rowCount);
		assertNullValues((DateMilliVector)root.getVector(vectors[5]), rowCount);
		assertNullValues((TimeStampVector)root.getVector(vectors[6]), rowCount);
		assertNullValues((VarBinaryVector)root.getVector(vectors[7]), rowCount);
		assertNullValues((VarCharVector)root.getVector(vectors[8]), rowCount);
		assertNullValues((VarBinaryVector)root.getVector(vectors[9]), rowCount);
		assertNullValues((VarCharVector)root.getVector(vectors[10]), rowCount);
		assertNullValues((VarCharVector)root.getVector(vectors[11]), rowCount);
		assertNullValues((BitVector)root.getVector(vectors[12]), rowCount);
    }
}

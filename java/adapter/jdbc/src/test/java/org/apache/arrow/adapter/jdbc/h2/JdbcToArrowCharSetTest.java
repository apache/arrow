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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertVarcharVectorValues;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.getCharArrayWithCharSet;

@RunWith(Parameterized.class)
public class JdbcToArrowCharSetTest extends AbstractJdbcToArrowTest {
    private static final String VARCHAR = "VARCHAR_FIELD13";
    private static final String CHAR = "CHAR_FIELD16";
    private static final String CLOB = "CLOB_FIELD15";
    
    private static final String[] testFiles = {
    		"h2/test1_charset_h2.yml", 
    		"h2/test1_charset_ch_h2.yml", 
    		"h2/test1_charset_jp_h2.yml", 
    		"h2/test1_charset_kr_h2.yml"
    };
    
    /**
     * Constructor which populate table object for each test iteration
     * @param table
     */
    public JdbcToArrowCharSetTest (Table table) {
        this.table = table;
    }
    
    /**
     * This method creates Connection object and DB table and also populate data into table for test
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Before
    public void setUp() throws SQLException, ClassNotFoundException {
    	String url = "jdbc:h2:mem:JdbcToArrowTest?characterEncoding=UTF-8";
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
     * This method returns collection of Table object for each test iteration
     * @return
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Parameters
    public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, IOException {
    	return Arrays.asList(prepareTestData(testFiles, JdbcToArrowCharSetTest.class));
    }

   /**
    * This method tests Chars, Varchars and Clob data with UTF-8 charset
    * @throws SQLException
    * @throws IOException
    * @throws ClassNotFoundException
    */
    @Test
    public void testCharSetBasedData() throws SQLException, IOException, ClassNotFoundException {
    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(),
    			new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())) {
    		
    		testDataSets(root);
    	}	
    }
    
    /**
     * This method tests Chars, Varchars and Clob data with UTF-8 charset using ResultSet
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    
    @Test
    public void testCharSetDataUsingResultSet() throws SQLException, IOException, ClassNotFoundException {
    	try (Statement stmt = conn.createStatement();
    			VectorSchemaRoot root = JdbcToArrow.sqlToArrow(stmt.executeQuery(table.getQuery()),
    				Calendar.getInstance())) {
    		testDataSets(root);
    	}
    }
    
    /**
     * This method tests Chars, Varchars and Clob data with UTF-8 charset using ResultSet and Allocator
     * @throws SQLException
     * @throws IOException
     * @throws ClassNotFoundException
     */
    
    @Test
    public void testCharSetDataUsingResultSetAndAllocator() throws SQLException, IOException, ClassNotFoundException {
    	try (Statement stmt = conn.createStatement();
    			VectorSchemaRoot root = JdbcToArrow.sqlToArrow(stmt.executeQuery(table.getQuery()),
    					new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance())) {
    		
    		testDataSets(root);
    	}	
    }
    
    /**
     * This method calls the assert methods for various DataSets
     * @param root
     */
    public void testDataSets(VectorSchemaRoot root) throws UnsupportedEncodingException{
    	assertVarcharVectorValues((VarCharVector) root.getVector(CLOB), table.getRowCount(),
    			getCharArrayWithCharSet(table.getValues(), CLOB, StandardCharsets.UTF_8));

    	assertVarcharVectorValues((VarCharVector) root.getVector(VARCHAR), table.getRowCount(),
    			getCharArrayWithCharSet(table.getValues(), VARCHAR, StandardCharsets.UTF_8));

    	assertVarcharVectorValues((VarCharVector) root.getVector(CHAR), table.getRowCount(),
    			getCharArrayWithCharSet(table.getValues(), CHAR, StandardCharsets.UTF_8));
    }
}

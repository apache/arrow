/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc.h2;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.*;


/**
 *
 */
@RunWith(Parameterized.class)
public class JdbcToArrowTest {

	private Connection conn = null;
	private Table table;
	
    private static final String BIGINT = "BIGINT_FIELD5";
    private static final String BINARY = "BINARY_FIELD12";
    private static final String BIT = "BIT_FIELD17";
    private static final String BLOB = "BLOB_FIELD14";
    private static final String BOOL = "BOOL_FIELD2";
    private static final String CHAR = "CHAR_FIELD16";
    private static final String CLOB = "CLOB_FIELD15";
    private static final String DATE = "DATE_FIELD10";
    private static final String DECIMAL = "DECIMAL_FIELD6";
    private static final String DOUBLE = "DOUBLE_FIELD7";
    private static final String INT = "INT_FIELD1";
    private static final String REAL = "REAL_FIELD8";
    private static final String SMALLINT = "SMALLINT_FIELD4";
    private static final String TIME = "TIME_FIELD9";
    private static final String TIMESTAMP = "TIMESTAMP_FIELD11";
    private static final String TINYINT = "TINYINT_FIELD3";
    private static final String VARCHAR = "VARCHAR_FIELD13";

    private static final String[] testFiles = {"h2/test1_all_datatypes_h2.yml"};
    
    /**
     * Constructor which populate table object for each test iteration
     * @param table
     */
    public JdbcToArrowTest(Table table) {
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
        			getCharArrays(table.getValues(), CLOB));

        	assertVarcharVectorValues((VarCharVector) root.getVector(VARCHAR), table.getRowCount(),
        			getCharArrays(table.getValues(), VARCHAR));
        	
        	assertVarcharVectorValues((VarCharVector) root.getVector(CHAR), table.getRowCount(),
        			getCharArrays(table.getValues(), CHAR));
        	
        	assertIntVectorValues((IntVector) root.getVector(INT), table.getRowCount(),
        			getIntValues(table.getValues(), INT));
        	
        	assertBitVectorValues((BitVector) root.getVector( BIT), table.getRowCount(),
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
    }
    
    private Integer [] getIntValues(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	Integer [] valueArr = new Integer [dataArr.length];
    	int i =0;
    	for(String data : dataArr) {
    		valueArr [i++] = Integer.parseInt(data);
    	}
    	return valueArr;
    }
    
    private Boolean [] getBooleanValues(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	Boolean [] valueArr = new Boolean [dataArr.length];
    	int i =0;
    	for(String data : dataArr) { 
    		valueArr [i++] = data.trim().equals("1");
    	}
    	return valueArr;
    }
    
    private BigDecimal [] getDecimalValues(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	BigDecimal [] valueArr = new BigDecimal [dataArr.length];
    	int i =0;
    	for(String data : dataArr) {
    		valueArr[i++] = new BigDecimal(data);
    	}
    	return valueArr;
    }

    private Double [] getDoubleValues(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	Double [] valueArr = new Double [dataArr.length];
    	int i =0;
    	for(String data : dataArr) {
    		valueArr [i++] = Double.parseDouble(data);
    	}
    	return valueArr;
    }  
    private Float [] getFloatValues(String[] values, String dataType) { 
    	String[] dataArr= getValues(values, dataType);
    	Float [] valueArr = new Float [dataArr.length];
    	int i =0;
    	for(String data : dataArr) {
    		valueArr [i++] = Float.parseFloat(data);
    	}
    	return valueArr;
    } 
    private Long [] getLongValues(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	Long [] valueArr = new Long [dataArr.length];
    	int i =0;
    	for(String data : dataArr) {    
    		valueArr [i++] = Long.parseLong(data);
    	}
    	return valueArr;
    }
    
    private byte [][] getCharArrays(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	byte [][] valueArr = new byte [dataArr.length][];
    	int i =0;
    	for(String data : dataArr) {     
    		valueArr [i++] = data.trim().getBytes();
    	}
    	return valueArr;
    }   
    
    private byte [][] getBinaryValues(String[] values, String dataType) {
    	String[] dataArr= getValues(values, dataType);
    	byte [][] valueArr = new byte [dataArr.length][];
    	int i =0;
    	for(String data : dataArr) {     
    		valueArr [i++] = hexStringToByteArray(data.trim());
    	}
    	return valueArr;
    }  
    
    private String [] getValues(String[] values, String dataType) {
    	String value = "";
    	for(String val : values) {
    		if(val.startsWith(dataType)) {
    			value = val.split("=")[1];
    			break;
    		}
    	}
    	return value.split(",");
    }
}

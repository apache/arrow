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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.Arrays;
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
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBigIntVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBitBooleanVectorValues;
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
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertNullValues;

@RunWith(Parameterized.class)
public class JdbcToArrowTestH2Datatypes {
	   private static Connection conn = null; 
	   private static Table table;
	   private int [] intValues;
	   private int [] boolValues;
	   private BigDecimal [] decimalValues;
	   private double [] doubleValues;
	   private int [] tinyIntValues;
	   private int [] smallIntValues;
	   private int [] bigIntValues;
	   private long [] timeValues;
	   private long [] dateValues;
	   private long [] timestampValues;
	   private float [] realValues;
	   private byte [][] byteValues;
	   private byte [][] varCharValues;
	   private byte [][] charValues;
	   private byte [][] clobValues;

	   public JdbcToArrowTestH2Datatypes (Table data) {
	    	super();
	    	this.intValues = data.getInts();
	    	this.boolValues = data.getBooleans();
	    	this.decimalValues = data.getDecimals();
	    	this.doubleValues = data.getDoubles();
	    	this.tinyIntValues = data.getTinyInts();
	    	this.smallIntValues = data.getSmallInts();
	    	this.bigIntValues = data.getBigInts();
	    	this.timeValues = data.getTimes();
	    	this.dateValues = data.getDates();
	    	this.timestampValues = data.getTimestamps();
	    	this.realValues = data.getReals();
	    	this.byteValues = data.getHexStringAsByte();
	    	this.varCharValues = data.getVarCharAsByte();
	    	this.charValues = data.getCharAsByte();
	    	this.clobValues = data.getClobAsByte();
	    	
	    }
	    
	    @Parameters
	    public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException {
	    	setUp();
	    	return Arrays.asList(new Object[][]{{table}});
	    }
	    
	    @Test
	    public void testDBValues() {
	    	try {
		    	sqlToArrowTestInt();
		    	sqlToArrowTestBool();
			    sqlToArrowTestTinyInts();  
			    sqlToArrowTestSmallInts();  
			    sqlToArrowTestBigInts();
		    	sqlToArrowTestBigDecimals();
		    	sqlToArrowTestDoubles();
		    	sqlToArrowTestRealValues();
			    sqlToArrowTestTimeValues(); 
			    sqlToArrowTestDateValues();
			    sqlToArrowTestTimestampValues();
			    sqlToArrowTestByteValues(); 
			    sqlToArrowTestVarCharValues(); 
			    sqlToArrowTestCharValues();
			    sqlToArrowTestBlobValues(); 
			    sqlToArrowTestClobValues();
			    sqlToArrowTestBits();
			    sqlToArrowTestNullValues();
	    	} catch (SQLException sqe) {
	    		sqe.printStackTrace();
	    	}
	    }
	    
	    public void sqlToArrowTestInt() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("INT_FIELD1", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), intValues.length, intValues);
	        }	
	    }
	    
	    public void sqlToArrowTestBool() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("BOOL_FIELD2", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertBitBooleanVectorValues((BitVector)root.getVector("BOOL_FIELD2"), boolValues.length, boolValues);
	        }	
	    }
	    
	    public void sqlToArrowTestTinyInts() throws SQLException {
	    	try(VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("TINYINT_FIELD3", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertTinyIntVectorValues((TinyIntVector)root.getVector("TINYINT_FIELD3"), tinyIntValues.length, tinyIntValues);
	        } 	
	    }

	    public void sqlToArrowTestSmallInts() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("SMALLINT_FIELD4", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertSmallIntVectorValues((SmallIntVector)root.getVector("SMALLINT_FIELD4"), smallIntValues.length, smallIntValues);
	    	}	
	    }

	    public void sqlToArrowTestBigInts() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("BIGINT_FIELD5", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertBigIntVectorValues((BigIntVector)root.getVector("BIGINT_FIELD5"), bigIntValues.length, bigIntValues);
	        }	
	    }

	    public void sqlToArrowTestBigDecimals() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("DECIMAL_FIELD6", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertDecimalVectorValues((DecimalVector)root.getVector("DECIMAL_FIELD6"), decimalValues.length, decimalValues);
	        } 	
	    }

	    public void sqlToArrowTestDoubles() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("DOUBLE_FIELD7", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertFloat8VectorValues((Float8Vector)root.getVector("DOUBLE_FIELD7"), doubleValues.length, doubleValues);
	        }	
	    }
   
	    public void sqlToArrowTestRealValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("REAL_FIELD8", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertFloat4VectorValues((Float4Vector)root.getVector("REAL_FIELD8"), realValues.length, realValues);
	    	}	
	    }
  
	    public void sqlToArrowTestTimeValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("TIME_FIELD9", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertTimeVectorValues((TimeMilliVector)root.getVector("TIME_FIELD9"), timeValues.length, timeValues);
	    	}	
	    }
  
	    public void sqlToArrowTestDateValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("DATE_FIELD10", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertDateVectorValues((DateMilliVector)root.getVector("DATE_FIELD10"), dateValues.length, dateValues);
	    	}	
	    }
   
	    public void sqlToArrowTestTimestampValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("TIMESTAMP_FIELD11", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertTimeStampVectorValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), timestampValues.length, timestampValues);
	    	}	
	    }
  
	    public void sqlToArrowTestByteValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("BINARY_FIELD12", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BINARY_FIELD12"), byteValues.length, byteValues);

	    	}	
	    }
  
	    public void sqlToArrowTestVarCharValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("VARCHAR_FIELD13", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarcharVectorValues((VarCharVector)root.getVector("VARCHAR_FIELD13"), varCharValues.length, varCharValues);
	    	}	
	    }

	    public void sqlToArrowTestBlobValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("BLOB_FIELD14", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BLOB_FIELD14"), byteValues.length, byteValues);
	    	}	
	    }
 
	    public void sqlToArrowTestClobValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("CLOB_FIELD15", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarcharVectorValues((VarCharVector)root.getVector("CLOB_FIELD15"), clobValues.length, clobValues);
	    	}	
	    }
  
	    public void sqlToArrowTestCharValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("CHAR_FIELD16", true), new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarcharVectorValues((VarCharVector)root.getVector("CHAR_FIELD16"), charValues.length, charValues);
	    	}	
	    }

	    public void sqlToArrowTestBits() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("BIT_FIELD17", true), new RootAllocator(Integer.MAX_VALUE))){
	            assertBitBooleanVectorValues((BitVector)root.getVector("BIT_FIELD17"), boolValues.length, boolValues);
	        }	
	    }

	    public void sqlToArrowTestNullValues() throws SQLException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery ("all", false, "INT_FIELD1"), new RootAllocator(Integer.MAX_VALUE))){
	    		assertNullValues((IntVector)root.getVector("INT_FIELD1"), 5);
	    		assertNullValues((BitVector)root.getVector("BOOL_FIELD2"), 5);
	    		assertNullValues((TinyIntVector)root.getVector("TINYINT_FIELD3"), 5);
	    		assertNullValues((SmallIntVector)root.getVector("SMALLINT_FIELD4"), 5);
	    		assertNullValues((BigIntVector)root.getVector("BIGINT_FIELD5"), 5);
	    		assertNullValues((DecimalVector)root.getVector("DECIMAL_FIELD6"), 5);
	    		assertNullValues((Float8Vector)root.getVector("DOUBLE_FIELD7"), 5);
	    		assertNullValues((Float4Vector)root.getVector("REAL_FIELD8"), 5);
	    		assertNullValues((TimeMilliVector)root.getVector("TIME_FIELD9"), 5);
	    		assertNullValues((DateMilliVector)root.getVector("DATE_FIELD10"), 5);
	    		assertNullValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), 5);
	    		assertNullValues((VarBinaryVector)root.getVector("BINARY_FIELD12"), 5);
	    		assertNullValues((VarCharVector)root.getVector("VARCHAR_FIELD13"), 5);
	    		assertNullValues((VarBinaryVector)root.getVector("BLOB_FIELD14"), 5);
	    		assertNullValues((VarCharVector)root.getVector("CLOB_FIELD15"), 5);
	    		assertNullValues((VarCharVector)root.getVector("CHAR_FIELD16"), 5);
	    		assertNullValues((BitVector)root.getVector("BIT_FIELD17"), 5);
	    	} 	
	    }
	    
	    @Before
	    public void createTestData() throws SQLException{
	        try (Statement stmt = conn.createStatement();){
	        	stmt.executeUpdate(table.getCreate());
	            for (String insert: table.getData()) {
	                stmt.executeUpdate(insert);
	            }
	        } 
	    }
	    
	    @After
	    public void destroy() throws SQLException {
	        if (conn != null) { 
	            conn.close();
	            conn = null;
	        }
	    }
	    
	    private static void setUp() throws SQLException, ClassNotFoundException {
	        String url = "jdbc:h2:mem:JdbcToArrowTest";
	        String driver = "org.h2.Driver";
	        Class.forName(driver);
	        conn = DriverManager.getConnection(url);
	        table = getTable ("h2/test1_all_datatypes_h2.yml");
	    }
	    
	    private static Table getTable (String ymlFilePath) {
	    	Table table = null;
	    	try {
	    		table  = new ObjectMapper(new YAMLFactory()).readValue(
	    				JdbcToArrowTestH2Datatypes.class.getClassLoader().getResourceAsStream(ymlFilePath),
	                Table.class);
	        } catch (JsonMappingException jme) {
	        	jme.printStackTrace();
	        }  catch (JsonParseException jpe) {
	        	jpe.printStackTrace();
	        } catch (IOException  ioe) {
	        	ioe.printStackTrace();
	        }
	    	return table;
	    }

	    private String getQuery (String columnName, boolean isNotNull) {
		   return getQuery (null, isNotNull, columnName);
	    }
	    
	    private  String getQuery (String allColumns, boolean isNotNull, String column) {
		   StringBuffer query = new StringBuffer(allColumns != null ? table.getSelectQuery("*") : table.getSelectQuery(column));
		   query.append(isNotNull ? " where " + column + " is not null;" : " where " + column + " is null;");
		   return query.toString();
	    }
}


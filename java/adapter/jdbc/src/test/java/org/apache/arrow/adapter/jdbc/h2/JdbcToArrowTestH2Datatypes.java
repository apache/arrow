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
/*
import org.junit.runners.Parameterized.Parameters;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.arrow.adapter.jdbc.Table;
import java.util.Arrays;
import java.util.Collection;

import java.util.Calendar;
import java.util.TimeZone;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
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
import java.math.BigDecimal;
import org.junit.After;
import org.junit.Before;
import java.sql.Statement;

import org.junit.Test;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBigIntVectorValues;
//import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBitBooleanVectorValues;
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
*/
@RunWith(Parameterized.class)
public class JdbcToArrowTestH2Datatypes {
	/*
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
	   private long [] pstTimeValues;
	   private long [] estTimeValues;
	   private long [] gmtTimeValues;
	   private long [] pstDateValues;
	   private long [] estDateValues;
	   private long [] gmtDateValues;
	   private long [] pstTimestampValues;
	   private long [] estTimestampValues;
	   private long [] gmtTimestampValues;
	   
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
	    	this.pstTimeValues = data.getPstTime();
	    	this.estTimeValues = data.getEstTime();
	    	this.gmtTimeValues = data.getGmtTime();
	    	this.pstDateValues = data.getPstDate();
	    	this.estDateValues = data.getEstDate();
	    	this.gmtDateValues = data.getGmtDate();
	    	this.pstTimestampValues = data.getPstTimestamp();
	    	this.estTimestampValues = data.getEstTimestamp() ;
	    	this.gmtTimestampValues = data.getGmtTimestamp();

	    }

	    @Parameters
	    public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException, JsonMappingException, JsonParseException, IOException {
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
	    		sqlToArrowTestValuesWithPSTTimeZone();
	    		sqlToArrowTestValuesWithESTTimeZone();
	    		sqlToArrowTestValuesWithGMTTimeZone();
	    		sqlToArrowTestSelectedColumnsNullValues();
	    	} catch (SQLException sqe) {
	    		sqe.printStackTrace();
	    	} catch (Exception e) {
	    		e.printStackTrace();
	    	}
	    }

	    public void sqlToArrowTestInt() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("INT_FIELD1 = 101", true, "INT_FIELD1"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), intValues.length, intValues);
	        }	
	    }

	    public void sqlToArrowTestBool() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("BOOL_FIELD2 = 1", true, "BOOL_FIELD2"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertBitBooleanVectorValues((BitVector)root.getVector("BOOL_FIELD2"), boolValues.length, boolValues);
	        }	
	    }

	    public void sqlToArrowTestTinyInts() throws SQLException, IOException {
	    	try(VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("TINYINT_FIELD3 = 45", true, "TINYINT_FIELD3"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertTinyIntVectorValues((TinyIntVector)root.getVector("TINYINT_FIELD3"), tinyIntValues.length, tinyIntValues);
	        } 	
	    }

	    public void sqlToArrowTestSmallInts() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery("SMALLINT_FIELD4 = 12000", true, "SMALLINT_FIELD4"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertSmallIntVectorValues((SmallIntVector)root.getVector("SMALLINT_FIELD4"), smallIntValues.length, smallIntValues);
	    	}	
	    }

	    public void sqlToArrowTestBigInts() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "BIGINT_FIELD5"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertBigIntVectorValues((BigIntVector)root.getVector("BIGINT_FIELD5"), bigIntValues.length, bigIntValues);
	        }	
	    }

	    public void sqlToArrowTestBigDecimals() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "DECIMAL_FIELD6"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertDecimalVectorValues((DecimalVector)root.getVector("DECIMAL_FIELD6"), decimalValues.length, decimalValues);
	        } 	
	    }

	    public void sqlToArrowTestDoubles() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "DOUBLE_FIELD7"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertFloat8VectorValues((Float8Vector)root.getVector("DOUBLE_FIELD7"), doubleValues.length, doubleValues);
	        }	
	    }

	    public void sqlToArrowTestRealValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "REAL_FIELD8"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertFloat4VectorValues((Float4Vector)root.getVector("REAL_FIELD8"), realValues.length, realValues);
	    	}	
	    }

	    public void sqlToArrowTestTimeValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "TIME_FIELD9"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertTimeVectorValues((TimeMilliVector)root.getVector("TIME_FIELD9"), timeValues.length, timeValues);
	    	}	
	    }
  
	    public void sqlToArrowTestDateValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "DATE_FIELD10"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertDateVectorValues((DateMilliVector)root.getVector("DATE_FIELD10"), dateValues.length, dateValues);
	    	}	
	    }   
   
	    public void sqlToArrowTestTimestampValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "TIMESTAMP_FIELD11"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertTimeStampVectorValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), timestampValues.length, timestampValues);
	    	}	
	    }
	    
	    public void sqlToArrowTestByteValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "BINARY_FIELD12"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BINARY_FIELD12"), byteValues.length, byteValues);

	    	}	
	    }

	    public void sqlToArrowTestVarCharValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "VARCHAR_FIELD13"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarcharVectorValues((VarCharVector)root.getVector("VARCHAR_FIELD13"), varCharValues.length, varCharValues);
	    	}	
	    }

	    public void sqlToArrowTestBlobValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "BLOB_FIELD14"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarBinaryVectorValues((VarBinaryVector)root.getVector("BLOB_FIELD14"), byteValues.length, byteValues);
	    	}	
	    }
 
	    public void sqlToArrowTestClobValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "CLOB_FIELD15"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarcharVectorValues((VarCharVector)root.getVector("CLOB_FIELD15"), clobValues.length, clobValues);
	    	}	
	    }

	    public void sqlToArrowTestCharValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "CHAR_FIELD16"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	    		assertVarcharVectorValues((VarCharVector)root.getVector("CHAR_FIELD16"), charValues.length, charValues);
	    	}	
	    }

	    public void sqlToArrowTestBits() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery(true, "BIT_FIELD17"), 
	    			new RootAllocator(Integer.MAX_VALUE))){
	            assertBitBooleanVectorValues((BitVector)root.getVector("BIT_FIELD17"), boolValues.length, boolValues);
	        }	
	    }

	    public void sqlToArrowTestNullValues() throws SQLException, IOException {
	    	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, 
	    			getQuery (false, table.getAllColumns()), new RootAllocator(Integer.MAX_VALUE))){
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
	    
	    public void sqlToArrowTestValuesWithPSTTimeZone() throws SQLException, IOException {
		    try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, 
		    		getQuery(" rownum < 6 ", true, "TIME_FIELD9", "DATE_FIELD10", "TIMESTAMP_FIELD11"), 
		    		new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance(TimeZone.getTimeZone("PST")))){
		    	assertTimeVectorValues((TimeMilliVector)root.getVector("TIME_FIELD9"), pstTimeValues.length, pstTimeValues);
		    	assertDateVectorValues((DateMilliVector)root.getVector("DATE_FIELD10"), pstDateValues.length, pstDateValues);
		    	assertTimeStampVectorValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), pstTimestampValues.length, 
		    			pstTimestampValues);
	    	}
	    }
	    
	    public void sqlToArrowTestValuesWithESTTimeZone() throws SQLException, IOException {
		    try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, 
		    		getQuery(" rownum < 6 ", true, "TIME_FIELD9", "DATE_FIELD10", "TIMESTAMP_FIELD11"), 
		    		new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance(TimeZone.getTimeZone("EST")))){
		    	assertTimeVectorValues((TimeMilliVector)root.getVector("TIME_FIELD9"), estTimeValues.length, estTimeValues);
		    	assertDateVectorValues((DateMilliVector)root.getVector("DATE_FIELD10"), estDateValues.length, estDateValues);
		    	assertTimeStampVectorValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), estTimestampValues.length, 
		    			estTimestampValues);
	    	} 
	    }	    
	    
	    public void sqlToArrowTestValuesWithGMTTimeZone() throws SQLException, IOException {
		    try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, 
		    		getQuery(" rownum < 6 ", true, "TIME_FIELD9", "DATE_FIELD10", "TIMESTAMP_FIELD11"), 
		    		new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance(TimeZone.getTimeZone("GMT")))){
		    	assertTimeVectorValues((TimeMilliVector)root.getVector("TIME_FIELD9"), gmtTimeValues.length, gmtTimeValues);
		    	assertDateVectorValues((DateMilliVector)root.getVector("DATE_FIELD10"), gmtDateValues.length, gmtDateValues);
		    	assertTimeStampVectorValues((TimeStampVector)root.getVector("TIMESTAMP_FIELD11"), gmtTimestampValues.length, 
		    			gmtTimestampValues);
	    	} 
	    }
	    
	    public void sqlToArrowTestSelectedColumnsNullValues() throws SQLException, IOException {
	    	  	try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, getQuery ("INT_FIELD1 = 102", true, table.getAllColumns()), 
	    	  			new RootAllocator(Integer.MAX_VALUE))){
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
	    
	    private static void setUp() throws SQLException, ClassNotFoundException, JsonMappingException, JsonParseException, IOException {
	        String url = "jdbc:h2:mem:JdbcToArrowTest";
	        String driver = "org.h2.Driver";
	        Class.forName(driver);
	        conn = DriverManager.getConnection(url);
	        table = getTable ("h2/test1_all_datatypes_h2.yml");
	    }

	    private static Table getTable (String ymlFilePath) throws JsonMappingException, JsonParseException, IOException {
	    	return new ObjectMapper(new YAMLFactory()).readValue(
	    				JdbcToArrowTestH2Datatypes.class.getClassLoader().getResourceAsStream(ymlFilePath),
	                Table.class);
	    }
	    
	    private String getQuery (boolean isNotNull, String... columns) {
	    	return getQuery("", isNotNull, columns);

	    } 
	    
	    private String getQuery (String whereClause, boolean isNotNull, String... columns) {
	    	return getQuery( whereClause, isNotNull, 0, columns);

	    } 
	    
	    private String getQuery(String whereClause, boolean isNotNull,  int index, String... columns) {
	        StringBuilder query = new  StringBuilder(String.format("select %s from %s ", 
//	        		columns.length > 1 ? String.join(",", columns) : columns[index], table.getName()));
	        		columns.length > 1 ? join(",", columns) : columns[index], table.getName()));
	        query.append(whereClause != null && !whereClause.isEmpty() ? " where " + whereClause + " and " : " where ");
	        columns = columns.length == 1 && columns[0].contains(",") ? columns[0].split(",") : columns;
	        query.append(isNotNull ? columns[index] + " is not null;" : columns[index] + " is null;");
	        
	        return query.toString();
	    }

		private static String join(String delimeter, String... columns) {
			if (columns.length == 1) {
				return columns[0];
			}
			StringBuilder builder = new StringBuilder();
			int i = 0;
			for (String column: columns) {
				if (i > 0) {
					builder.append(delimeter).append(" ");
				}
				builder.append(column);
				i++;
			}
			return builder.toString();
		}
		
		*/
}


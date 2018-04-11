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
import org.apache.arrow.adapter.jdbc.TestData;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertBitBooleanVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDecimalVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertFloat8VectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertIntVectorValues;

@RunWith(Parameterized.class)
public class JdbcToArrowTestMultipleDatatypes {
	   private static Connection conn = null; 
	   private static Table table;
	   // This is the parameter which will be populated with data from YAML file using  @Parameters annotated method via constructor 
	   public int [] intValues;
	   public int [] boolValues;
	   public BigDecimal [] decimalValues;
	   public double [] doubleValues;
	   
	   // This is the constructor which will add data to @Parameter annotated variable
	    public JdbcToArrowTestMultipleDatatypes (TestData data) {
	    	super();
	    	this.intValues = data.getIntValues();
	    	this.boolValues = data.getBoolValues();
	    	this.decimalValues = data.getDecimalValues();
	    	this.doubleValues = data.getDoubleValues();
	    }
	    
	    // This is the method which is annotated for injecting values into the test class constructor 
	    @Parameters
	    public static Collection<Object[]> getTestData() throws SQLException, ClassNotFoundException {
	    	setUp();
	    	
	    	TestData data = new TestData();
	    	data.setIntValues(table.getIntegers());
	    	data.setBoolValues(table.getBooleans());
	    	data.setDecimalValues(table.getDecimals());
	    	data.setDoubleValues(table.getDoubles());
	    	
	    	return Arrays.asList(new Object[][]{{data}});
	    }
	    
	    // Calling individual test methods in this method, because need to call @After destroy() method only once to close connection 
	    //finally after all the tests are performed, this is just temporarily done to show how can we perform multiple datatype tests
	    @Test
	    public void testDBValues() {
	    	sqlToArrowTestInt();
	    	sqlToArrowTestBool();
	    	sqlToArrowTestBigDecimals();
	    	sqlToArrowTestDoubles();
	    	sqlToArrowTestMultipleDataTypes();
	    }
	    
	    // Testing Int values
	    public void sqlToArrowTestInt() {
	    	try {
	            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getIntQuery(), new RootAllocator(Integer.MAX_VALUE));
	            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), intValues.length, intValues);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }	
	    }
	    
	    // Testing Boolean values
	    public void sqlToArrowTestBool() {
	    	try {
	            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getBooleanQuery(), new RootAllocator(Integer.MAX_VALUE));
	            assertBitBooleanVectorValues((BitVector)root.getVector("BOOL_FIELD2"), boolValues.length, boolValues);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }	
	    }

	    // Testing BigDecimal values
	    public void sqlToArrowTestBigDecimals() {
	    	try {
	            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getDecimalQuery(), new RootAllocator(Integer.MAX_VALUE));
	            assertDecimalVectorValues((DecimalVector)root.getVector("DECIMAL_FIELD6"), decimalValues.length, decimalValues);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }	
	    }
	    
	    // Testing Double values
	    public void sqlToArrowTestDoubles() {
	    	try {
	            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getDoubleQuery(), new RootAllocator(Integer.MAX_VALUE));
	            assertFloat8VectorValues((Float8Vector)root.getVector("DOUBLE_FIELD7"), doubleValues.length, doubleValues);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }	
	    }
	    
	    // Testing Multiple Data Types values
	    public void sqlToArrowTestMultipleDataTypes() {
	    	try {
	            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE));
	            
	            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), intValues.length, intValues);
	            assertBitBooleanVectorValues((BitVector)root.getVector("BOOL_FIELD2"), boolValues.length, boolValues);
	            assertDecimalVectorValues((DecimalVector)root.getVector("DECIMAL_FIELD6"), decimalValues.length, decimalValues);
	            assertFloat8VectorValues((Float8Vector)root.getVector("DOUBLE_FIELD7"), doubleValues.length, doubleValues);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }	
	    }
	    
	    public static void setUp() throws SQLException, ClassNotFoundException {
		        String url = "jdbc:h2:mem:JdbcToArrowTest";
		        String driver = "org.h2.Driver";
		       
		        Class.forName(driver);
		        conn = DriverManager.getConnection(url);
		        table = getTable ("h2/test1_all_datatypes_h2.yml");
	    }

	    @After
	    public void destroy() throws SQLException {
	        if (conn != null) { 
	            conn.close();
	            conn = null;
	        }
	    }
	    	    
	    protected static Table getTable (String ymlFilePath) {
	    	Table table = null;
	    	try {
	    		table = new ObjectMapper(new YAMLFactory()).readValue(
	    				JdbcToArrowTestMultipleDatatypes.class.getClassLoader().getResourceAsStream(ymlFilePath),
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
	    
	    @Before
	    public void createTestData() {

	        Statement stmt = null;
	        try {
	            //create the table and insert the data and once done drop the table
	            stmt = conn.createStatement();
	            stmt.executeUpdate(table.getCreate());

	            for (String insert: table.getData()) {
	                stmt.executeUpdate(insert);
	            }

	        } catch (SQLException e) {
	            e.printStackTrace();
	        } finally {
	        	//JdbcToArrowUtils.closeStatement(stmt);
	        }
	    }
}


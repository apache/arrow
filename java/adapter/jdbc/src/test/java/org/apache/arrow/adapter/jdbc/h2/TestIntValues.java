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
import java.util.List;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Test;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertIntVectorValues;

@RunWith(Parameterized.class)
public class TestIntValues {
	   private static Connection conn = null; 
	   private static Table table;
	   
	   // This is the parameter which will be populated with data from YAML file using  @Parameters annotated method via constructor 
	   public int [] intValues;
	    
	   // This is the constructor which will add data to @Parameter annotated variable
	    public TestIntValues (IntegerValues integerValues) {
	    	super();
	    	this.intValues = integerValues.intValues;
	    }
	    
	    // This is the method which is annotated for injecting values into the test class constructor 
	    @Parameters
	    public static Collection<Object[]> getTestData() throws Exception {
	    	// Calling setUp() method here in place of using it with @Before annotation because this method is called before @Before annotated 
	    	// method and we need Table object and also Connection object Because for this class creating table and populating it 
	    	// with data using YML file only which should be done automatically as per the comment so this will be change and it place of
	    	// this we can call method to just create Table object (for ex - getTable(ymlFileNamePath))
	    	setUp();
	    	
	    	final List<Integer> valueList = new ArrayList<>();
	    	
	    	Arrays.stream(table.getData()).parallel().forEach(value -> {
	    		valueList.add(Integer.parseInt(value));
	    	});
	    	return Arrays.asList(new Object[][]{
	    	      {new IntegerValues(valueList.stream().mapToInt(Integer::intValue).toArray())}});
	    }
	    
	    // This is the test method after making changes as per comment
	    @Test
	    public void sqlToArrowTestInt() {
	    	try {
	            VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(), new RootAllocator(Integer.MAX_VALUE));
	            assertIntVectorValues((IntVector)root.getVector("INT_FIELD1"), intValues.length, intValues);
	        } catch (Exception e) {
	        	e.printStackTrace();
	        }	
	    }
	    	    
	   // @Before
	    public static void setUp() throws Exception {
	    	//try {
		        String url = "jdbc:h2:mem:JdbcToArrowTest";
		        String driver = "org.h2.Driver";
		       
		        Class.forName(driver);
		        conn = DriverManager.getConnection(url);
		        
		        table = getTable ("h2/test1_int_h2.yml");
		    	createTestData(conn, table);
	    	//} catch ()
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
	    				TestIntValues.class.getClassLoader().getResourceAsStream(ymlFilePath),
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
	    
	    protected static void  createTestData(Connection conn, Table table) {

	        Statement stmt = null;
	        try {
	            //create the table and insert the data and once done drop the table
	            stmt = conn.createStatement();
	            stmt.executeUpdate(table.getCreate());

	            for (String insert: table.getInserts()) {
	                stmt.executeUpdate(insert);
	            }

	        } catch (SQLException e) {
	            e.printStackTrace();
	        } finally {
	        	//JdbcToArrowUtils.closeStatement(stmt);
	        }
	    }
    
}

class IntegerValues {

	  public int [] intValues;
	 
	  public IntegerValues(int [] intValues) {
	    this.intValues = intValues;
	  }
}

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

import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertDateVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeStampVectorValues;
import static org.apache.arrow.adapter.jdbc.JdbcToArrowTestHelper.assertTimeVectorValues;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.TimeZone;
import org.apache.arrow.adapter.jdbc.AbstractJdbcToArrowTest;
import org.apache.arrow.adapter.jdbc.JdbcToArrow;
import org.apache.arrow.adapter.jdbc.Table;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * 
 * JUnit Test class to test TimeZone based Date, Time and Timestamp datatypes for H2 database
 *
 */
@RunWith(Parameterized.class)
public class JdbcToArrowTimeZoneTest extends AbstractJdbcToArrowTest {

    private static final String EST_DATE = "est_date";
    private static final String EST_TIME = "est_time";
    private static final String EST_TIMESTAMP = "est_timestamp";
    private static final String GMT_DATE = "gmt_date";
    private static final String GMT_TIME = "gmt_time";
    private static final String GMT_TIMESTAMP = "gmt_timestamp";
    private static final String PST_DATE = "pst_date";
    private static final String PST_TIME = "pst_time";
    private static final String PST_TIMESTAMP = "pst_timestamp";
    
    private static final String[] testFiles = {
        "h2/test1_est_date_h2.yml",
        "h2/test1_est_time_h2.yml",
        "h2/test1_est_timestamp_h2.yml",
        "h2/test1_gmt_date_h2.yml",
        "h2/test1_gmt_time_h2.yml",
        "h2/test1_gmt_timestamp_h2.yml",
        "h2/test1_pst_date_h2.yml",
        "h2/test1_pst_time_h2.yml",
        "h2/test1_pst_timestamp_h2.yml"
    };

    /**
     * Constructor which populate table object for each test iteration
     * @param table
     */
    public JdbcToArrowTimeZoneTest (Table table) {
        this.table = table;
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
        return Arrays.asList(prepareTestData(testFiles, JdbcToArrowTimeZoneTest.class));
    }
    
    /**
     * This method tests TimeZone based Date, Time and Timestamp datatypes for H2 database
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testTimeZoneBasedValues() throws SQLException, IOException {
    	 try (VectorSchemaRoot root = JdbcToArrow.sqlToArrow(conn, table.getQuery(),
                 new RootAllocator(Integer.MAX_VALUE), Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone())))) {
    		 
    		 testDataSets(root);
         }
    } 
    
    /**
     * This method tests TimeZone based Date, Time and Timestamp datatypes for H2 database using ResultSet
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testTimeZoneBasedValuesUsingResultSet() throws SQLException, IOException {
    	try (Statement stmt = conn.createStatement();
    			VectorSchemaRoot root = JdbcToArrow.sqlToArrow(stmt.executeQuery(table.getQuery()),
    			Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone())))) {

    			testDataSets(root);
    	}
    }
    
    /**
     * This method tests TimeZone based Date, Time and Timestamp datatypes for H2 database using ResultSet and Allocator
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testTimeZoneBasedValuesUsingResultSetAndAllocator() throws SQLException, IOException {
    	try (Statement stmt = conn.createStatement();
    			VectorSchemaRoot root = JdbcToArrow.sqlToArrow(stmt.executeQuery(table.getQuery()), new RootAllocator(Integer.MAX_VALUE),
    			Calendar.getInstance(TimeZone.getTimeZone(table.getTimezone())))) {
    			
    			testDataSets(root);
         }
    }
    
    /**
     * This method calls the assert methods for various DataSets
     * @param root
     */
    public void testDataSets(VectorSchemaRoot root) {
    	switch (table.getType()) {
	        case EST_DATE: case GMT_DATE: case PST_DATE:
	            assertDateVectorValues((DateMilliVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
	            break;
	        case EST_TIME: case GMT_TIME: case PST_TIME:
	            assertTimeVectorValues((TimeMilliVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
	            break;
	        case EST_TIMESTAMP: case GMT_TIMESTAMP: case PST_TIMESTAMP:
	            assertTimeStampVectorValues((TimeStampVector) root.getVector(table.getVector()), table.getValues().length, table.getLongValues());
	            break;
    	}
    }

}

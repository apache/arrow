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
import org.apache.arrow.adapter.jdbc.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.Assert.*;

/**
 * Test class for {@link ArrowDataFetcher}.
 */
public class ArrowDataFetcherTest extends AbstractJdbcToArrowTest {

    private Connection conn = null;
    private ObjectMapper mapper = null;

    @Before
    public void setUp() throws Exception {
        String url = "jdbc:h2:mem:ArrowDataFetcherTest";
        String driver = "org.h2.Driver";

        mapper = new ObjectMapper(new YAMLFactory());

        Class.forName(driver);

        conn = DriverManager.getConnection(url);
    }

    @After
    public void destroy() throws Exception {
        if (conn != null) {
            conn.close();
            conn = null;
        }
    }

    @Test
    public void commaSeparatedQueryColumnsTest() {
        try {
            ArrowDataFetcher.commaSeparatedQueryColumns(null);
        } catch (AssertionError error) {
            assertTrue(true);
        }
        assertEquals(" one ", ArrowDataFetcher.commaSeparatedQueryColumns("one"));
        assertEquals(" one, two ", ArrowDataFetcher.commaSeparatedQueryColumns("one", "two"));
        assertEquals(" one, two, three ", ArrowDataFetcher.commaSeparatedQueryColumns("one", "two", "three"));
    }

    @Test
    public void arrowFetcherAllColumnsLimitOffsetTest() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_int_h2.yml"),
                        Table.class);

        try {
            createTestData(conn, table);

            ArrowDataFetcher arrowDataFetcher = JdbcToArrow.jdbcArrowDataFetcher(conn, "table1");

            VectorSchemaRoot root = arrowDataFetcher.fetch(0, 10);

            int[] values = {
                    101, 101, 101, 101, 101, 101, 101, 101, 101, 101
            };
            JdbcToArrowTestHelper.assertIntVectorValues(root.getVector("INT_FIELD1"), 10, values);

            root = arrowDataFetcher.fetch(5, 5);

            JdbcToArrowTestHelper.assertIntVectorValues(root.getVector("INT_FIELD1"), 5, values);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }

    @Test
    public void arrowFetcherSingleColumnLimitOffsetTest() throws Exception {

        Table table =
                mapper.readValue(
                        this.getClass().getClassLoader().getResourceAsStream("h2/test1_int_h2.yml"),
                        Table.class);

        try {
            createTestData(conn, table);

            ArrowDataFetcher arrowDataFetcher = JdbcToArrow.jdbcArrowDataFetcher(conn, "table1");

            VectorSchemaRoot root = arrowDataFetcher.fetch(0, 10, "int_field1");

            int[] values = {
                    101, 101, 101, 101, 101, 101, 101, 101, 101, 101
            };
            JdbcToArrowTestHelper.assertIntVectorValues(root.getVector("INT_FIELD1"), 10, values);

            root = arrowDataFetcher.fetch(5, 5);

            JdbcToArrowTestHelper.assertIntVectorValues(root.getVector("INT_FIELD1"), 5, values);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTestData(conn, table);
        }

    }


}

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

package org.apache.arrow.adapter.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 *
 */
public class JdbcToArrowTest {

    private Connection conn = null;

    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        properties.load(this.getClass().getClassLoader().getResourceAsStream("db.properties"));

        Class.forName(properties.getProperty("driver"));

        conn = DriverManager
                .getConnection(properties.getProperty("url"), properties);;
    }

    @Test
    public void sqlToArrowTest() throws Exception {

        // create the table and insert data for the test
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
//            Table table = mapper.readValue(new File("/home/datum/codebase/arrow/java/adapter/jdbc/src/test/resources/test1_h2.yml"), Table.class);


            System.out.print(10);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {

        } finally {

        }

    }

    @After
    public void destroy() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

}

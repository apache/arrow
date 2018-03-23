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

import java.sql.Connection;
import java.sql.Statement;

/**
 * Class to abstract out some common test functionality for testing JDBC to Arrow.
 */
public abstract class AbstractJdbcToArrowTest {

    protected void createTestData(Connection conn, Table table) throws Exception {

        Statement stmt = null;
        try {
            //create the table and insert the data and once done drop the table
            stmt = conn.createStatement();
            stmt.executeUpdate(table.getCreate());

            for (String insert: table.getData()) {
                stmt.executeUpdate(insert);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }

    }


    protected void deleteTestData(Connection conn, Table table) throws Exception {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            stmt.executeUpdate(table.getDrop());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }
}

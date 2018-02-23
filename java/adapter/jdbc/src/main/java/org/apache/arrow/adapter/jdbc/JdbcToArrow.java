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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.sql.*;

/**
 *
 */
public class JdbcToArrow {

    /**
     * For the given SQL query, execute and fetch the data from Relational DB and convert it to Arrow objects.
     *
     * @param connection Database connection to be used. This method will not close the passed connection object. Since hte caller has passed
     *                   the connection object it's the responsibility of the caller to close or return the connection to the pool.
     * @param query The DB Query to fetch the data.
     * @return
     * @throws SQLException Propagate any SQL Exceptions to the caller after closing any resources opened such as ResultSet and Statment objects.
     */
    public static VectorSchemaRoot sqlToArrow(Connection connection, String query) throws Exception {

        RootAllocator rootAllocator = new RootAllocator(Integer.MAX_VALUE);

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            VectorSchemaRoot root = VectorSchemaRoot.create(
                    JdbcToArrowUtils.jdbcToArrowSchema(rsmd), rootAllocator);
            JdbcToArrowUtils.jdbcToArrowVectors(rs, root);
            return root;
        } catch (Exception exc) {
            // just throw it out after logging
            throw exc;
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }



}

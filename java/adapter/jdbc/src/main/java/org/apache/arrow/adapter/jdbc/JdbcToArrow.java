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
 * Utility class to convert JDBC objects to columnar Arrow format objects.
 *
 * This utility uses following data mapping to map JDBC/SQL datatype to Arrow data types.
 *
 * CHAR	--> ArrowType.Utf8
 * NCHAR	--> ArrowType.Utf8
 * VARCHAR --> ArrowType.Utf8
 * NVARCHAR --> ArrowType.Utf8
 * LONGVARCHAR --> ArrowType.Utf8
 * LONGNVARCHAR --> ArrowType.Utf8
 * NUMERIC --> ArrowType.Decimal(precision, scale)
 * DECIMAL --> ArrowType.Decimal(precision, scale)
 * BIT --> ArrowType.Bool
 * TINYINT --> ArrowType.Int(8, signed)
 * SMALLINT --> ArrowType.Int(16, signed)
 * INTEGER --> ArrowType.Int(32, signed)
 * BIGINT --> ArrowType.Int(64, signed)
 * REAL --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
 * FLOAT --> ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
 * DOUBLE --> ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
 * BINARY --> ArrowType.Binary
 * VARBINARY --> ArrowType.Binary
 * LONGVARBINARY --> ArrowType.Binary
 * DATE --> ArrowType.Date(DateUnit.MILLISECOND)
 * TIME --> ArrowType.Time(TimeUnit.MILLISECOND, 32)
 * TIMESTAMP --> ArrowType.Timestamp(TimeUnit.MILLISECOND, timezone=null)
 * CLOB --> ArrowType.Utf8
 * BLOB --> ArrowType.Binary
 *
 * @since 0.10.0
 * @see ArrowDataFetcher
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

        assert connection != null: "JDBC conncetion object can not be null";
        assert query != null && query.length() > 0: "SQL query can not be null or empty";

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
                stmt.close(); // test
            }
        }
    }

    /**
     * This method returns ArrowDataFetcher Object that can be used to fetch and iterate on the data in the given
     * database table.
     *
     * @param connection - Database connection Object
     * @param tableName - Table name from which records will be fetched
     *
     * @return ArrowDataFetcher - Instance of ArrowDataFetcher which can be used to get Arrow Vector obejcts by calling its functionality
     */
    public static ArrowDataFetcher jdbcArrowDataFetcher(Connection connection, String tableName) {
        assert connection != null: "JDBC conncetion object can not be null";
        assert tableName != null && tableName.length() > 0: "Table name can not be null or empty";

        return new ArrowDataFetcher(connection, tableName);
    }

}

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

import org.apache.arrow.vector.VectorSchemaRoot;

import java.sql.Connection;

/**
 *
 */
public class ArrowDataFetcher {

    private static final String all_columns_query = "select * from %s limit %d offset %d";
    private static final String custom_columns_query = "select %s from %s limit %d offset %d";
    private Connection connection;
    private String tableName;

    public ArrowDataFetcher(Connection connection, String tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public VectorSchemaRoot fetch(int offset, int limit, String... columns) throws Exception {
        assert columns != null && columns.length > 0 : "columns can't be empty!";
        assert limit > 0 : "limit needs to be greater that 0";
        assert offset >= 0 : "offset needs to be greater than or equal to 0";

        return JdbcToArrow.sqlToArrow(connection,
                String.format(custom_columns_query, commaSeparatedQueryColumns(columns),
                        tableName, limit, offset));
    }

    public VectorSchemaRoot fetch(int offset, int limit) throws Exception {
        assert limit > 0 : "limit needs to be greater that 0";
        assert offset >= 0 : "offset needs to be greater than or equal to 0";

        return JdbcToArrow.sqlToArrow(connection, String.format(all_columns_query, tableName, limit, offset));
    }

    public static String commaSeparatedQueryColumns(String... columns) {
        assert columns != null && columns.length > 0 : "columns can't be empty!";

        StringBuilder columnBuilder = new StringBuilder();
        boolean insertComma = false;
        for (String s: columns) {
            if (insertComma) {
                columnBuilder.append(',');
            }
            columnBuilder.append(' ').append(s);
            insertComma = true;
        }
        columnBuilder.append(' ');
        return columnBuilder.toString();
    }

}



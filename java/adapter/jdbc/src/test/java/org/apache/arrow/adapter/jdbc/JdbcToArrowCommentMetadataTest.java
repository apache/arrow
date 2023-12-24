/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.adapter.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JdbcToArrowCommentMetadataTest {

  private static final String COMMENT = "comment"; //use this metadata key for interoperability with Spark StructType
  private Connection conn = null;

  /**
   * This method creates Connection object and DB table and also populate data into table for test.
   *
   * @throws SQLException on error
   * @throws ClassNotFoundException on error
   */
  @Before
  public void setUp() throws SQLException, ClassNotFoundException {
    String url = "jdbc:h2:mem:JdbcToArrowTest?characterEncoding=UTF-8;INIT=runscript from 'classpath:/h2/comment.sql'";
    String driver = "org.h2.Driver";
    Class.forName(driver);
    conn = DriverManager.getConnection(url);
  }

  @After
  public void tearDown() throws SQLException {
    if (conn != null) {
      conn.close();
      conn = null;
    }
  }

  private static Field field(String name, boolean nullable, ArrowType type, Map<String, String> metadata) {
    return new Field(name, new FieldType(nullable, type, null, metadata), Collections.emptyList());
  }

  private static Map<String, String> metadata(String... entries) {
    if (entries.length % 2 != 0) {
      throw new IllegalArgumentException("Map must have equal number of keys and values");
    }

    final Map<String, String> result = new HashMap<>();
    for (int i = 0; i < entries.length; i += 2) {
      result.put(entries[i], entries[i + 1]);
    }
    return result;
  }

  @Test
  public void schemaComment() throws Exception {
    boolean includeMetadata = false;
    Schema schema = getSchemaWithCommentFromQuery(includeMetadata);
    Schema expectedSchema = new Schema(Arrays.asList(
        field("ID", false, Types.MinorType.BIGINT.getType(),
            metadata("comment", "Record identifier")),
        field("NAME", true, Types.MinorType.VARCHAR.getType(),
            metadata("comment", "Name of record")),
        field("COLUMN1", true, Types.MinorType.BIT.getType(),
            metadata()),
        field("COLUMNN", true, Types.MinorType.INT.getType(),
            metadata("comment", "Informative description of columnN"))
        ), metadata("comment", "This is super special table with valuable data"));
    assertThat(schema).isEqualTo(expectedSchema);
  }

  @Test
  public void schemaCommentWithDatabaseMetadata() throws Exception {
    boolean includeMetadata = true;
    Schema schema = getSchemaWithCommentFromQuery(includeMetadata);
    Schema expectedSchema = new Schema(Arrays.asList(
        field("ID", false, Types.MinorType.BIGINT.getType(),
            metadata(
                "SQL_CATALOG_NAME", "JDBCTOARROWTEST?CHARACTERENCODING=UTF-8",
                "SQL_SCHEMA_NAME", "PUBLIC",
                "SQL_TABLE_NAME", "TABLE1",
                "SQL_COLUMN_NAME", "ID",
                "SQL_TYPE", "BIGINT",
                "comment", "Record identifier"
            )),
        field("NAME", true, Types.MinorType.VARCHAR.getType(),
            metadata(
                "SQL_CATALOG_NAME", "JDBCTOARROWTEST?CHARACTERENCODING=UTF-8",
                "SQL_SCHEMA_NAME", "PUBLIC",
                "SQL_TABLE_NAME", "TABLE1",
                "SQL_COLUMN_NAME", "NAME",
                "SQL_TYPE", "CHARACTER VARYING",
                "comment", "Name of record")),
        field("COLUMN1", true, Types.MinorType.BIT.getType(),
            metadata(
                "SQL_CATALOG_NAME", "JDBCTOARROWTEST?CHARACTERENCODING=UTF-8",
                "SQL_SCHEMA_NAME", "PUBLIC",
                "SQL_TABLE_NAME", "TABLE1",
                "SQL_COLUMN_NAME", "COLUMN1",
                "SQL_TYPE", "BOOLEAN")),
        field("COLUMNN", true, Types.MinorType.INT.getType(),
            metadata(
                "SQL_CATALOG_NAME", "JDBCTOARROWTEST?CHARACTERENCODING=UTF-8",
                "SQL_SCHEMA_NAME", "PUBLIC",
                "SQL_TABLE_NAME", "TABLE1",
                "SQL_COLUMN_NAME", "COLUMNN",
                "SQL_TYPE", "INTEGER",
                "comment", "Informative description of columnN"))
    ), metadata("comment", "This is super special table with valuable data"));
    assertThat(schema).isEqualTo(expectedSchema);
    /* corresponding Apache Spark DDL after conversion:
        ID BIGINT NOT NULL COMMENT 'Record identifier',
        NAME STRING COMMENT 'Name of record',
        COLUMN1 BOOLEAN,
        COLUMNN INT COMMENT 'Informative description of columnN'
     */
    assertThat(schema).isEqualTo(expectedSchema);
  }

  private Schema getSchemaWithCommentFromQuery(boolean includeMetadata) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    try (Statement statement = conn.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("select * from table1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<Integer, Map<String, String>> columnCommentByColumnIndex = getColumnComments(metaData, resultSetMetaData);

        String tableName = getTableNameFromResultSetMetaData(resultSetMetaData);
        String tableComment = getTableComment(metaData, tableName);
        JdbcToArrowConfig config = new JdbcToArrowConfigBuilder()
                .setAllocator(new RootAllocator()).setSchemaMetadata(Collections.singletonMap(COMMENT, tableComment))
                .setColumnMetadataByColumnIndex(columnCommentByColumnIndex).setIncludeMetadata(includeMetadata).build();
        return JdbcToArrowUtils.jdbcToArrowSchema(resultSetMetaData, config);
      }
    }
  }

  private String getTableNameFromResultSetMetaData(ResultSetMetaData resultSetMetaData) throws SQLException {
    Set<String> tablesFromQuery = new HashSet<>();
    for (int idx = 1, columnCount = resultSetMetaData.getColumnCount(); idx <= columnCount; idx++) {
      String tableName = resultSetMetaData.getTableName(idx);
      if (tableName != null && !tableName.isEmpty()) {
        tablesFromQuery.add(tableName);
      }
    }
    if (tablesFromQuery.size() == 1) {
      return tablesFromQuery.iterator().next();
    }
    throw new RuntimeException("Table metadata is absent or ambiguous");
  }

  private Map<Integer, Map<String, String>> getColumnComments(DatabaseMetaData metaData,
                                                 ResultSetMetaData resultSetMetaData) throws SQLException {
    Map<Integer, Map<String, String>> columnCommentByColumnIndex = new HashMap<>();
    for (int columnIdx = 1, columnCount = resultSetMetaData.getColumnCount(); columnIdx <= columnCount; columnIdx++) {
      String columnComment = getColumnComment(metaData, resultSetMetaData.getTableName(columnIdx),
              resultSetMetaData.getColumnName(columnIdx));
      if (columnComment != null && !columnComment.isEmpty()) {
        columnCommentByColumnIndex.put(columnIdx, Collections.singletonMap(COMMENT, columnComment));
      }
    }
    return columnCommentByColumnIndex;
  }

  private String getTableComment(DatabaseMetaData metaData, String tableName) throws SQLException {
    if (tableName == null || tableName.isEmpty()) {
      return null;
    }
    String comment = null;
    int rowCount = 0;
    try (ResultSet tableMetadata = metaData.getTables(null, null, tableName, null)) {
      if (tableMetadata.next()) {
        comment = tableMetadata.getString("REMARKS");
        rowCount++;
      }
    }
    if (rowCount == 1) {
      return comment;
    }
    if (rowCount > 1) {
      throw new RuntimeException("Multiple tables found for table name");
    }
    throw new RuntimeException("Table comment not found");
  }

  private String getColumnComment(DatabaseMetaData metaData, String tableName, String columnName) throws SQLException {
    try (ResultSet tableMetadata = metaData.getColumns(null, null, tableName, columnName)) {
      if (tableMetadata.next()) {
        return tableMetadata.getString("REMARKS");
      }
    }
    return null;
  }

  private String getExpectedSchema(String expectedResource) throws java.io.IOException, java.net.URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(Objects.requireNonNull(
            JdbcToArrowCommentMetadataTest.class.getResource(expectedResource)).toURI())), StandardCharsets.UTF_8);
  }
}

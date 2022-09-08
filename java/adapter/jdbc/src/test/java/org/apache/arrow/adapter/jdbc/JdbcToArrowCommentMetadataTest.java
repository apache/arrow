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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ObjectMapperFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectWriter;

public class JdbcToArrowCommentMetadataTest {

  private final ObjectWriter schemaSerializer = ObjectMapperFactory.newObjectMapper().writerWithDefaultPrettyPrinter();
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

  @Test
  public void schemaComment() throws Exception {
    boolean includeMetadata = false;
    String schemaJson = schemaSerializer.writeValueAsString(getSchemaWithCommentFromQuery(includeMetadata));
    String expectedSchema = getExpectedSchema("/h2/expectedSchemaWithComments.json");
    assertThat(schemaJson).isEqualTo(expectedSchema);
  }

  @Test
  public void schemaCommentWithDatabaseMetadata() throws Exception {
    boolean includeMetadata = true;
    String schemaJson = schemaSerializer.writeValueAsString(getSchemaWithCommentFromQuery(includeMetadata));
    String expectedSchema = getExpectedSchema("/h2/expectedSchemaWithCommentsAndJdbcMeta.json");
    assertThat(schemaJson).isEqualTo(expectedSchema);
  }

  private Schema getSchemaWithCommentFromQuery(boolean includeMetadata) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();
    try (Statement statement = conn.createStatement()) {
      try (ResultSet resultSet = statement.executeQuery("select * from table1")) {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Map<Integer, String> columnCommentByColumnIndex = getColumnComments(metaData, resultSetMetaData);

        final String tableComment = getTableComment(metaData, "TABLE1");
        JdbcToArrowConfig config = new JdbcToArrowConfigBuilder()
                .setAllocator(new RootAllocator()).setSchemaComment(tableComment)
                .setColumnCommentByColumnIndex(columnCommentByColumnIndex).setIncludeMetadata(includeMetadata).build();
        return JdbcToArrowUtils.jdbcToArrowSchema(resultSetMetaData, config);
      }
    }
  }

  private Map<Integer, String> getColumnComments(DatabaseMetaData metaData,
                                                 ResultSetMetaData resultSetMetaData) throws SQLException {
    Map<Integer, String> columnCommentByColumnIndex = new HashMap<>();
    for (int columnIdx = 1, columnCount = resultSetMetaData.getColumnCount(); columnIdx <= columnCount; columnIdx++) {
      String columnComment = getColumnComment(metaData, resultSetMetaData.getTableName(columnIdx),
              resultSetMetaData.getColumnName(columnIdx));
      if (columnComment != null && !columnComment.isEmpty()) {
        columnCommentByColumnIndex.put(columnIdx, columnComment);
      }
    }
    return columnCommentByColumnIndex;
  }

  private String getTableComment(DatabaseMetaData metaData, String tableName) throws SQLException {
    try (ResultSet tableMetadata = metaData.getTables("%", "%", tableName, null)) {
      if (tableMetadata.next()) {
        return tableMetadata.getString("REMARKS");
      }
    }
    throw new RuntimeException("Table comment not found");
  }

  private String getColumnComment(DatabaseMetaData metaData, String tableName, String columnName) throws SQLException {
    try (ResultSet tableMetadata = metaData.getColumns("%", "%", tableName, columnName)) {
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

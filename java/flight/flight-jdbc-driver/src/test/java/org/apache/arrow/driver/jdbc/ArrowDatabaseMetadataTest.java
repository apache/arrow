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

package org.apache.arrow.driver.jdbc;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import org.apache.arrow.driver.jdbc.test.FlightServerTestRule;
import org.apache.arrow.driver.jdbc.test.adhoc.MockFlightSqlProducer;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.protobuf.Message;

/**
 * Class containing the tests from the {@link ArrowDatabaseMetadata}.
 */
public class ArrowDatabaseMetadataTest {
  private static final Random RANDOM = new Random(10);

  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createNewTestRule(FLIGHT_SQL_PRODUCER);

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  private static Connection connection;

  @BeforeClass
  public static void setup() throws SQLException {
    connection = FLIGHT_SERVER_TEST_RULE.getConnection();
    final Message commandGetCatalogs = CommandGetCatalogs.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetCatalogsResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator)) {
        final VarCharVector varCharVector = (VarCharVector) root.getVector("catalog_name");
        final int rows = 10;
        range(0, rows).forEach(i -> varCharVector.setSafe(i, new Text(format("catalog #%d", i))));
        root.setRowCount(rows);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetCatalogs, commandGetCatalogsResultProducer);

    final Message commandGetTableTypes = CommandGetTableTypes.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetTableTypesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLE_TYPES_SCHEMA, allocator)) {
        final VarCharVector varCharVector = (VarCharVector) root.getVector("table_type");
        final int rows = 10;
        range(0, rows).forEach(i -> varCharVector.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(rows);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTableTypes, commandGetTableTypesResultProducer);

    final Message commandGetTables = CommandGetTables.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetTablesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)) {
        final VarCharVector varCharVectorCatalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector varCharVectorSchemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector varCharVectorTableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector varCharVectorTableType = (VarCharVector) root.getVector("table_type");
        final int rows = 10;
        range(0, rows)
            .peek(i -> varCharVectorCatalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> varCharVectorSchemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> varCharVectorTableName.setSafe(i, new Text(format("table_name #%d", i))))
            .forEach(i -> varCharVectorTableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(rows);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTables, commandGetTablesResultProducer);

    final Message commandGetSchemas = CommandGetSchemas.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetSchemasResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
        final VarCharVector varCharVectorCatalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector varCharVectorSchemaName = (VarCharVector) root.getVector("schema_name");
        final int rows = 10;
        range(0, rows)
            .peek(i -> varCharVectorCatalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .forEach(i -> varCharVectorSchemaName.setSafe(i, new Text(format("schema_name #%d", i))));
        root.setRowCount(rows);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetSchemas, commandGetSchemasResultProducer);

    final Message commandGetExportedKeys = CommandGetExportedKeys.newBuilder().setTable("Test").build();
    final Message commandGetImportedKeys = FlightSql.CommandGetImportedKeys.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetExportedAndImportedKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_IMPORTED_AND_EXPORTED_KEYS_SCHEMA,
               allocator)) {
        final VarCharVector varCharVectorPKCatalogName = (VarCharVector) root.getVector("pk_catalog_name");
        final VarCharVector varCharVectorPKSchemaName = (VarCharVector) root.getVector("pk_schema_name");
        final VarCharVector varCharVectorPKTableName = (VarCharVector) root.getVector("pk_table_name");
        final VarCharVector varCharVectorPKColumnName = (VarCharVector) root.getVector("pk_column_name");
        final VarCharVector varCharVectorFKCatalogName = (VarCharVector) root.getVector("fk_catalog_name");
        final VarCharVector varCharVectorFKSchemaName = (VarCharVector) root.getVector("fk_schema_name");
        final VarCharVector varCharVectorFKTableName = (VarCharVector) root.getVector("fk_table_name");
        final VarCharVector varCharVectorFKColumnName = (VarCharVector) root.getVector("fk_column_name");
        final IntVector intVectorKeySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector varCharVectorFKKeyName = (VarCharVector) root.getVector("fk_key_name");
        final VarCharVector varCharVectorPKKeyName = (VarCharVector) root.getVector("pk_key_name");
        final UInt8Vector uInt8VectorVectorUpdateRule = (UInt8Vector) root.getVector("update_rule");
        final UInt8Vector uInt8VectorVectorDeleteRule = (UInt8Vector) root.getVector("delete_rule");
        final int rows = 10;
        range(0, rows)
            .peek(i -> varCharVectorPKCatalogName.setSafe(i, new Text(format("pk_catalog_name #%d", i))))
            .peek(i -> varCharVectorPKSchemaName.setSafe(i, new Text(format("pk_schema_name #%d", i))))
            .peek(i -> varCharVectorPKTableName.setSafe(i, new Text(format("pk_table_name #%d", i))))
            .peek(i -> varCharVectorPKColumnName.setSafe(i, new Text(format("pk_column_name #%d", i))))
            .peek(i -> varCharVectorFKCatalogName.setSafe(i, new Text(format("fk_catalog_name #%d", i))))
            .peek(i -> varCharVectorFKSchemaName.setSafe(i, new Text(format("fk_schema_name #%d", i))))
            .peek(i -> varCharVectorFKTableName.setSafe(i, new Text(format("fk_table_name #%d", i))))
            .peek(i -> varCharVectorFKColumnName.setSafe(i, new Text(format("fk_column_name #%d", i))))
            .peek(i -> intVectorKeySequence.setSafe(i, i))
            .peek(i -> varCharVectorFKKeyName.setSafe(i, new Text(format("fk_key_name #%d", i))))
            .peek(i -> varCharVectorPKKeyName.setSafe(i, new Text(format("pk_key_name #%d", i))))
            .peek(i -> uInt8VectorVectorUpdateRule.setSafe(i, i))
            .forEach(i -> uInt8VectorVectorDeleteRule.setSafe(i, i));
        root.setRowCount(rows);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetExportedKeys, commandGetExportedAndImportedKeysResultProducer);
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetImportedKeys, commandGetExportedAndImportedKeysResultProducer);

    final Message commandGetPrimaryKeys = FlightSql.CommandGetPrimaryKeys.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetPrimaryKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_PRIMARY_KEYS_SCHEMA, allocator)) {
        final VarCharVector varCharVectorCatalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector varCharVectorSchemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector varCharVectorTableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector varCharVectorColumnName = (VarCharVector) root.getVector("column_name");
        final IntVector intVectorKeySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector varCharVectorKeyName = (VarCharVector) root.getVector("key_name");
        final int rows = 10;
        range(0, rows)
            .peek(i -> varCharVectorCatalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> varCharVectorSchemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> varCharVectorTableName.setSafe(i, new Text(format("table_name #%d", i))))
            .peek(i -> varCharVectorColumnName.setSafe(i, new Text(format("column_name #%d", i))))
            .peek(i -> intVectorKeySequence.setSafe(i, i))
            .forEach(i -> varCharVectorKeyName.setSafe(i, new Text(format("key_name #%d", i))));
        root.setRowCount(rows);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetPrimaryKeys, commandGetPrimaryKeysResultProducer);

  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(connection, FLIGHT_SERVER_TEST_RULE, FLIGHT_SQL_PRODUCER);
  }

  @Test
  public void testCatalog() throws SQLException {
    final List<List<String>> expectedCatalogs =
        range(0, 10).mapToObj(i -> format("catalog #%d", i)).map(Collections::singletonList).collect(toList());
    final List<List<String>> actualCatalogs = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      while (resultSet.next()) {
        final List<String> catalogs = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          catalogs.add(resultSet.getString(column + 1));
        }
        actualCatalogs.add(catalogs);
      }
    }
    collector.checkThat(actualCatalogs, is(expectedCatalogs));
  }

  @Test
  public void testTableTypes() throws SQLException {
    final List<List<String>> expectedTableTypes =
        range(0, 10).mapToObj(i -> format("table_type #%d", i)).map(Collections::singletonList).collect(toList());
    final List<List<String>> actualTableTypes = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      while (resultSet.next()) {
        final List<String> tableTypes = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          tableTypes.add(resultSet.getString(column + 1));
        }
        actualTableTypes.add(tableTypes);
      }
    }
    collector.checkThat(actualTableTypes, is(expectedTableTypes));
  }

  @Test
  public void testTables() throws SQLException {
    final List<List<String>> expectedTables =
        range(0, 10)
            .mapToObj(i -> new String[] {
                format("catalog_name #%d", i),
                format("schema_name #%d", i),
                format("table_name #%d", i),
                format("table_type #%d", i),
                // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
                null, null, null, null, null, null})
            .map(Arrays::asList)
            .collect(toList());
    final List<List<String>> actualTables = new ArrayList<>();
    // FIXME Seems to be broken. Should accept null results...
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
      while (resultSet.next()) {
        final List<String> tables = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          tables.add(resultSet.getString(column + 1));
        }
        actualTables.add(tables);
      }
    }
    collector.checkThat(actualTables, is(expectedTables));
  }

  @Test
  @Ignore // FIXME TODO
  public void testSchemas() throws SQLException {
    final List<ArrayList<String>> expectedSchemas =
        range(0, 10).mapToObj(
                i -> new ArrayList<>(Arrays.asList(format("schema_name #%d", i), format("catalog_name #%d", i))))
            .collect(toList());
    final List<List<String>> actualSchemas = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      while (resultSet.next()) {
        final List<String> schemas = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          schemas.add(resultSet.getString(column + 1));
        }
        actualSchemas.add(schemas);
      }
    }
    collector.checkThat(actualSchemas, is(expectedSchemas));
  }

  @Test
  @Ignore // FIXME TODO
  public void testExportedKeys() throws SQLException {
    final List<ArrayList<?>> expectedSchemas =
        range(0, 10).mapToObj(i -> new ArrayList<>(Arrays.asList(
            format("pk_catalog_name #%d", i),
            format("pk_schema_name #%d", i),
            format("pk_table_name #%d", i),
            format("pk_column_name #%d", i),
            format("fk_catalog_name #%d", i),
            format("fk_schema_name #%d", i),
            format("fk_table_name #%d", i),
            format("fk_column_name #%d", i),
            i,
            format("fk_key_name #%d", i),
            format("pk_key_name #%d", i),
            i,
            i
        ))).collect(toList());
    final List<List<String>> actualSchemas = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, "Test")) {
      while (resultSet.next()) {
        final List<String> schemas = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          schemas.add(resultSet.getString(column + 1));
        }
        actualSchemas.add(schemas);
      }
    }
    collector.checkThat(actualSchemas, is(expectedSchemas));
  }

  @Test
  @Ignore // FIXME TODO
  public void testImportedKeys() throws SQLException {
    final List<ArrayList<?>> expectedSchemas =
        range(0, 10).mapToObj(i -> new ArrayList<>(Arrays.asList(
            format("pk_catalog_name #%d", i),
            format("pk_schema_name #%d", i),
            format("pk_table_name #%d", i),
            format("pk_column_name #%d", i),
            format("fk_catalog_name #%d", i),
            format("fk_schema_name #%d", i),
            format("fk_table_name #%d", i),
            format("fk_column_name #%d", i),
            i,
            format("fk_key_name #%d", i),
            format("pk_key_name #%d", i),
            i,
            i
        ))).collect(toList());
    final List<List<String>> actualSchemas = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, "Test")) {
      while (resultSet.next()) {
        final List<String> schemas = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          schemas.add(resultSet.getString(column + 1));
        }
        actualSchemas.add(schemas);
      }
    }
    collector.checkThat(actualSchemas, is(expectedSchemas));
  }

  @Test
  @Ignore // FIXME TODO
  public void testPrimaryKeys() throws SQLException {
    final List<ArrayList<?>> expectedSchemas =
        range(0, 10).mapToObj(i -> new ArrayList<>(Arrays.asList(
            format("catalog_name #%d", i),
            format("schema_name #%d", i),
            format("table_name #%d", i),
            format("column_name #%d", i),
            i,
            format("key_name #%d", i)
        ))).collect(toList());
    final List<List<String>> actualSchemas = new ArrayList<>();
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, "Test")) {
      while (resultSet.next()) {
        final List<String> schemas = new ArrayList<>();
        for (int column = 0; column < resultSet.getMetaData().getColumnCount(); column++) {
          schemas.add(resultSet.getString(column + 1));
        }
        actualSchemas.add(schemas);
      }
    }
    collector.checkThat(actualSchemas, is(expectedSchemas));
  }
}


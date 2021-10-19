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

import static com.google.protobuf.ByteString.copyFrom;
import static java.lang.String.format;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BIT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.JAVA_OBJECT;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.arrow.driver.jdbc.test.adhoc.MockFlightSqlProducer.serializeSchema;
import static org.apache.arrow.driver.jdbc.utils.DatabaseMetadataDenseUnionUtils.setDataForBigIntField;
import static org.apache.arrow.driver.jdbc.utils.DatabaseMetadataDenseUnionUtils.setDataForBooleanField;
import static org.apache.arrow.driver.jdbc.utils.DatabaseMetadataDenseUnionUtils.setDataForIntField;
import static org.apache.arrow.driver.jdbc.utils.DatabaseMetadataDenseUnionUtils.setDataForUtf8Field;
import static org.apache.arrow.driver.jdbc.utils.DatabaseMetadataDenseUnionUtils.setDataVarCharListField;
import static org.apache.arrow.driver.jdbc.utils.DatabaseMetadataDenseUnionUtils.setIntToIntListMapField;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.createBitmaskFromEnums;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.apache.arrow.driver.jdbc.test.FlightServerTestRule;
import org.apache.arrow.driver.jdbc.test.adhoc.MockFlightSqlProducer;
import org.apache.arrow.driver.jdbc.utils.ResultSetTestUtils;
import org.apache.arrow.flight.FlightProducer.ServerStreamListener;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetCatalogs;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetExportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetImportedKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetPrimaryKeys;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetSchemas;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTableTypes;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandGetTables;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedElementActions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedPositionedCommands;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedResultSetType;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedUnions;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportsConvert;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlTransactionIsolationLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedAnsi92SqlGrammarLevel;
import org.apache.arrow.flight.sql.impl.FlightSql.SupportedSqlGrammar;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

/**
 * Class containing the tests from the {@link ArrowDatabaseMetadata}.
 */
public class ArrowDatabaseMetadataTest {
  public static final boolean EXPECTED_MAX_ROW_SIZE_INCLUDES_BLOBS = false;
  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createNewTestRule(FLIGHT_SQL_PRODUCER);
  private static final int ROW_COUNT = 10;
  private static final List<List<Object>> EXPECTED_GET_CATALOGS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> format("catalog #%d", i))
          .map(Object.class::cast)
          .map(Collections::singletonList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_TABLE_TYPES_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> format("table_type #%d", i))
          .map(Object.class::cast)
          .map(Collections::singletonList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_TABLES_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("schema_name #%d", i),
              format("table_name #%d", i),
              format("table_type #%d", i),
              // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
              null, null, null, null, null, null})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_SCHEMAS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("schema_name #%d", i),
              format("catalog_name #%d", i)})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
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
              (byte) i,
              (byte) i,
              // TODO Add this field to FlightSQL, as it's currently not possible to fetch it.
              null})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<List<Object>> EXPECTED_PRIMARY_KEYS_RESULTS =
      range(0, ROW_COUNT)
          .mapToObj(i -> new Object[] {
              format("catalog_name #%d", i),
              format("schema_name #%d", i),
              format("table_name #%d", i),
              format("column_name #%d", i),
              i,
              format("key_name #%d", i)})
          .map(Arrays::asList)
          .collect(toList());
  private static final List<String> FIELDS_GET_IMPORTED_EXPORTED_KEYS = ImmutableList.of(
      "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME",
      "PKCOLUMN_NAME", "FKTABLE_CAT", "FKTABLE_SCHEM",
      "FKTABLE_NAME", "FKCOLUMN_NAME", "KEY_SEQ",
      "FK_NAME", "PK_NAME", "UPDATE_RULE", "DELETE_RULE",
      "DEFERRABILITY");
  private static final String TARGET_TABLE = "TARGET_TABLE";
  private static final String EXPECTED_DATABASE_PRODUCT_NAME = "Test Server Name";
  private static final String EXPECTED_DATABASE_PRODUCT_VERSION = "v0.0.1-alpha";
  private static final String EXPECTED_IDENTIFIER_QUOTE_STRING = "\"";
  private static final boolean EXPECTED_IS_READ_ONLY = true;
  private static final String EXPECTED_SQL_KEYWORDS = "ADD, ADD CONSTRAINT, ALTER, ALTER TABLE, ANY, USER, TABLE";
  private static final String EXPECTED_NUMERIC_FUNCTIONS = "ABS(), ACOS(), ASIN(), ATAN(), CEIL(), CEILING(), COT()";
  private static final String EXPECTED_STRING_FUNCTIONS = "ASCII, CHAR, CHARINDEX, CONCAT, CONCAT_WS, FORMAT, LEFT";
  private static final String EXPECTED_SYSTEM_FUNCTIONS = "CAST, CONVERT, CHOOSE, ISNULL, IS_NUMERIC, IIF, TRY_CAST";
  private static final String EXPECTED_TIME_DATE_FUNCTIONS = "GETDATE(), DATEPART(), DATEADD(), DATEDIFF()";
  private static final String EXPECTED_SEARCH_STRING_ESCAPE = "\\";
  private static final String EXPECTED_EXTRA_NAME_CHARACTERS = "";
  private static final boolean EXPECTED_SUPPORTS_COLUMN_ALIASING = true;
  private static final boolean EXPECTED_NULL_PLUS_NULL_IS_NULL = true;
  private static final boolean EXPECTED_SQL_SUPPORTS_CONVERT = true;
  private static final boolean EXPECTED_INVALID_SQL_SUPPORTS_CONVERT = false;
  private static final boolean EXPECTED_SUPPORTS_TABLE_CORRELATION_NAMES = true;
  private static final boolean EXPECTED_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES = false;
  private static final boolean EXPECTED_EXPRESSIONS_IN_ORDER_BY = true;
  private static final boolean EXPECTED_SUPPORTS_ORDER_BY_UNRELATED = true;
  private static final boolean EXPECTED_SUPPORTS_GROUP_BY = true;
  private static final boolean EXPECTED_SUPPORTS_GROUP_BY_UNRELATED = true;
  private static final boolean EXPECTED_SUPPORTS_LIKE_ESCAPE_CLAUSE = true;
  private static final boolean EXPECTED_NON_NULLABLE_COLUMNS = true;
  private static final boolean EXPECTED_MINIMUM_SQL_GRAMMAR = true;
  private static final boolean EXPECTED_CORE_SQL_GRAMMAR = true;
  private static final boolean EXPECTED_EXTEND_SQL_GRAMMAR = false;
  private static final boolean EXPECTED_ANSI92_ENTRY_LEVEL_SQL = true;
  private static final boolean EXPECTED_ANSI92_INTERMEDIATE_SQL = true;
  private static final boolean EXPECTED_ANSI92_FULL_SQL = false;
  private static final String EXPECTED_SCHEMA_TERM = "schema";
  private static final String EXPECTED_PROCEDURE_TERM = "procedure";
  private static final String EXPECTED_CATALOG_TERM = "catalog";
  private static final boolean EXPECTED_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY = true;
  private static final boolean EXPECTED_CATALOG_AT_START = true;
  private static final boolean EXPECTED_SCHEMAS_IN_PROCEDURE_CALLS = true;
  private static final boolean EXPECTED_SCHEMAS_IN_INDEX_DEFINITIONS = true;
  private static final boolean EXPECTED_SCHEMAS_IN_PRIVILEGE_DEFINITIONS = false;
  private static final boolean EXPECTED_CATALOGS_IN_INDEX_DEFINITIONS = true;
  private static final boolean EXPECTED_CATALOGS_IN_PRIVILEGE_DEFINITIONS = false;
  private static final boolean EXPECTED_POSITIONED_DELETE = true;
  private static final boolean EXPECTED_POSITIONED_UPDATE = false;
  private static final boolean EXPECTED_TYPE_FORWARD_ONLY = true;
  private static final boolean EXPECTED_TYPE_SCROLL_INSENSITIVE = true;
  private static final boolean EXPECTED_TYPE_SCROLL_SENSITIVE = false;
  private static final boolean EXPECTED_SELECT_FOR_UPDATE_SUPPORTED = false;
  private static final boolean EXPECTED_STORED_PROCEDURES_SUPPORTED = false;
  private static final boolean EXPECTED_SUBQUERIES_IN_COMPARISON = true;
  private static final boolean EXPECTED_SUBQUERIES_IN_EXISTS = false;
  private static final boolean EXPECTED_SUBQUERIES_IN_INS = false;
  private static final boolean EXPECTED_SUBQUERIES_IN_QUANTIFIEDS = false;
  private static final int EXPECTED_SUPPORTED_SUBQUERIES = 1;
  private static final boolean EXPECTED_CORRELATED_SUBQUERIES_SUPPORTED = true;
  private static final boolean EXPECTED_SUPPORTS_UNION = true;
  private static final boolean EXPECTED_SUPPORTS_UNION_ALL = true;
  private static final int EXPECTED_MAX_BINARY_LITERAL_LENGTH = 0;
  private static final int EXPECTED_MAX_CHAR_LITERAL_LENGTH = 0;
  private static final int EXPECTED_MAX_COLUMN_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_COLUMNS_IN_GROUP_BY = 0;
  private static final int EXPECTED_MAX_COLUMNS_IN_INDEX = 0;
  private static final int EXPECTED_MAX_COLUMNS_IN_ORDER_BY = 0;
  private static final int EXPECTED_MAX_COLUMNS_IN_SELECT = 0;
  private static final int EXPECTED_MAX_CONNECTIONS = 0;
  private static final int EXPECTED_MAX_CURSOR_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_INDEX_LENGTH = 0;
  private static final int EXPECTED_SCHEMA_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_PROCEDURE_NAME_LENGTH = 0;
  private static final int EXPECTED_MAX_CATALOG_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_ROW_SIZE = 0;
  private static final int EXPECTED_MAX_STATEMENT_LENGTH = 0;
  private static final int EXPECTED_MAX_STATEMENTS = 0;
  private static final int EXPECTED_MAX_TABLE_NAME_LENGTH = 1024;
  private static final int EXPECTED_MAX_TABLES_IN_SELECT = 0;
  private static final int EXPECTED_MAX_USERNAME_LENGTH = 1024;
  private static final int EXPECTED_DEFAULT_TRANSACTION_ISOLATION = 0;
  private static final boolean EXPECTED_TRANSACTIONS_SUPPORTED = false;
  private static final boolean EXPECTED_TRANSACTION_NONE = false;
  private static final boolean EXPECTED_TRANSACTION_READ_UNCOMMITTED = false;
  private static final boolean EXPECTED_TRANSACTION_READ_COMMITTED = true;
  private static final boolean EXPECTED_TRANSACTION_REPEATABLE_READ = false;
  private static final boolean EXPECTED_TRANSACTION_SERIALIZABLE = true;
  private static final boolean EXPECTED_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT = true;
  private static final boolean EXPECTED_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED = false;
  private static final boolean EXPECTED_BATCH_UPDATES_SUPPORTED = true;
  private static final boolean EXPECTED_SAVEPOINTS_SUPPORTED = false;
  private static final boolean EXPECTED_NAMED_PARAMETERS_SUPPORTED = false;
  private static final boolean EXPECTED_LOCATORS_UPDATE_COPY = true;
  private static final boolean EXPECTED_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED = false;
  private static final List<List<Object>> EXPECTED_GET_COLUMNS_RESULTS;
  private static Connection connection;

  static {
    List<Integer> expectedGetColumnsDataTypes = Arrays.asList(3, 93, 4);
    List<String> expectedGetColumnsTypeName = Arrays.asList("DECIMAL", "TIMESTAMP", "INTEGER");
    List<Integer> expectedGetColumnsRadix = Arrays.asList(10, null, 10);
    List<Integer> expectedGetColumnsColumnSize = Arrays.asList(5, 29, 10);
    List<Integer> expectedGetColumnsDecimalDigits = Arrays.asList(2, 9, 0);
    List<String> expectedGetColumnsIsNullable = Arrays.asList("YES", "YES", "NO");
    EXPECTED_GET_COLUMNS_RESULTS = range(0, ROW_COUNT * 3)
        .mapToObj(i -> new Object[] {
            format("catalog_name #%d", i / 3),
            format("schema_name #%d", i / 3),
            format("table_name%d", i / 3),
            format("column_%d", (i % 3) + 1),
            expectedGetColumnsDataTypes.get(i % 3),
            expectedGetColumnsTypeName.get(i % 3),
            expectedGetColumnsColumnSize.get(i % 3),
            null,
            expectedGetColumnsDecimalDigits.get(i % 3),
            expectedGetColumnsRadix.get(i % 3),
            !Objects.equals(expectedGetColumnsIsNullable.get(i % 3), "NO") ? 1 : 0,
            null, null, null, null, null,
            (i % 3) + 1,
            expectedGetColumnsIsNullable.get(i % 3),
            null, null, null, null,
            "", ""})
        .map(Arrays::asList)
        .collect(toList());
  }

  @Rule
  public final ErrorCollector collector = new ErrorCollector();
  public final ResultSetTestUtils resultSetTestUtils = new ResultSetTestUtils(collector);

  @BeforeClass
  public static void setUpBeforeClass() throws SQLException {
    connection = FLIGHT_SERVER_TEST_RULE.getConnection();
    final Message commandGetCatalogs = CommandGetCatalogs.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetCatalogsResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_CATALOGS_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        range(0, ROW_COUNT).forEach(i -> catalogName.setSafe(i, new Text(format("catalog #%d", i))));
        root.setRowCount(ROW_COUNT);
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
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT).forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(ROW_COUNT);
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
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .forEach(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTables, commandGetTablesResultProducer);

    final Message commandGetTablesWithSchema = CommandGetTables.newBuilder()
        .setIncludeSchema(true)
        .build();
    final Consumer<ServerStreamListener> commandGetTablesWithSchemaResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_TABLES_SCHEMA, allocator)) {
        final byte[] filledTableSchemaBytes =
            copyFrom(
                serializeSchema(new Schema(Arrays.asList(
                    Field.nullable("column_1", ArrowType.Decimal.createDecimal(5, 2, 128)),
                    Field.nullable("column_2", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC")),
                    Field.notNullable("column_3", Types.MinorType.INT.getType())))))
                .toByteArray();
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        final VarBinaryVector tableSchema = (VarBinaryVector) root.getVector("table_schema");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name%d", i))))
            .peek(i -> tableType.setSafe(i, new Text(format("table_type #%d", i))))
            .forEach(i -> tableSchema.setSafe(i, filledTableSchemaBytes));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetTablesWithSchema,
        commandGetTablesWithSchemaResultProducer);

    final Message commandGetSchemas = CommandGetSchemas.getDefaultInstance();
    final Consumer<ServerStreamListener> commandGetSchemasResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_SCHEMAS_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .forEach(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetSchemas, commandGetSchemasResultProducer);

    final Message commandGetExportedKeys = CommandGetExportedKeys.newBuilder().setTable(TARGET_TABLE).build();
    final Message commandGetImportedKeys = CommandGetImportedKeys.newBuilder().setTable(TARGET_TABLE).build();
    final Consumer<ServerStreamListener> commandGetExportedAndImportedKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_IMPORTED_KEYS_SCHEMA,
               allocator)) {
        final VarCharVector pkCatalogName = (VarCharVector) root.getVector("pk_catalog_name");
        final VarCharVector pkSchemaName = (VarCharVector) root.getVector("pk_schema_name");
        final VarCharVector pkTableName = (VarCharVector) root.getVector("pk_table_name");
        final VarCharVector pkColumnName = (VarCharVector) root.getVector("pk_column_name");
        final VarCharVector fkCatalogName = (VarCharVector) root.getVector("fk_catalog_name");
        final VarCharVector fkSchemaName = (VarCharVector) root.getVector("fk_schema_name");
        final VarCharVector fkTableName = (VarCharVector) root.getVector("fk_table_name");
        final VarCharVector fkColumnName = (VarCharVector) root.getVector("fk_column_name");
        final IntVector keySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector fkKeyName = (VarCharVector) root.getVector("fk_key_name");
        final VarCharVector pkKeyName = (VarCharVector) root.getVector("pk_key_name");
        final UInt1Vector updateRule = (UInt1Vector) root.getVector("update_rule");
        final UInt1Vector deleteRule = (UInt1Vector) root.getVector("delete_rule");
        range(0, ROW_COUNT)
            .peek(i -> pkCatalogName.setSafe(i, new Text(format("pk_catalog_name #%d", i))))
            .peek(i -> pkSchemaName.setSafe(i, new Text(format("pk_schema_name #%d", i))))
            .peek(i -> pkTableName.setSafe(i, new Text(format("pk_table_name #%d", i))))
            .peek(i -> pkColumnName.setSafe(i, new Text(format("pk_column_name #%d", i))))
            .peek(i -> fkCatalogName.setSafe(i, new Text(format("fk_catalog_name #%d", i))))
            .peek(i -> fkSchemaName.setSafe(i, new Text(format("fk_schema_name #%d", i))))
            .peek(i -> fkTableName.setSafe(i, new Text(format("fk_table_name #%d", i))))
            .peek(i -> fkColumnName.setSafe(i, new Text(format("fk_column_name #%d", i))))
            .peek(i -> keySequence.setSafe(i, i))
            .peek(i -> fkKeyName.setSafe(i, new Text(format("fk_key_name #%d", i))))
            .peek(i -> pkKeyName.setSafe(i, new Text(format("pk_key_name #%d", i))))
            .peek(i -> updateRule.setSafe(i, i))
            .forEach(i -> deleteRule.setSafe(i, i));
        root.setRowCount(ROW_COUNT);
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

    final Message commandGetPrimaryKeys = CommandGetPrimaryKeys.newBuilder().setTable(TARGET_TABLE).build();
    final Consumer<ServerStreamListener> commandGetPrimaryKeysResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(Schemas.GET_PRIMARY_KEYS_SCHEMA, allocator)) {
        final VarCharVector catalogName = (VarCharVector) root.getVector("catalog_name");
        final VarCharVector schemaName = (VarCharVector) root.getVector("schema_name");
        final VarCharVector tableName = (VarCharVector) root.getVector("table_name");
        final VarCharVector columnName = (VarCharVector) root.getVector("column_name");
        final IntVector keySequence = (IntVector) root.getVector("key_sequence");
        final VarCharVector keyName = (VarCharVector) root.getVector("key_name");
        range(0, ROW_COUNT)
            .peek(i -> catalogName.setSafe(i, new Text(format("catalog_name #%d", i))))
            .peek(i -> schemaName.setSafe(i, new Text(format("schema_name #%d", i))))
            .peek(i -> tableName.setSafe(i, new Text(format("table_name #%d", i))))
            .peek(i -> columnName.setSafe(i, new Text(format("column_name #%d", i))))
            .peek(i -> keySequence.setSafe(i, i))
            .forEach(i -> keyName.setSafe(i, new Text(format("key_name #%d", i))));
        root.setRowCount(ROW_COUNT);
        listener.start(root);
        listener.putNext();
      } catch (final Throwable throwable) {
        listener.error(throwable);
      } finally {
        listener.completed();
      }
    };
    FLIGHT_SQL_PRODUCER.addCatalogQuery(commandGetPrimaryKeys, commandGetPrimaryKeysResultProducer);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlServerNameProvider =
        (root, index) ->
            setDataForUtf8Field(root, index, SqlInfo.FLIGHT_SQL_SERVER_NAME, EXPECTED_DATABASE_PRODUCT_NAME);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.FLIGHT_SQL_SERVER_NAME, flightSqlServerNameProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlServerVersionProvider =
        (root, index) ->
            setDataForUtf8Field(root, index, SqlInfo.FLIGHT_SQL_SERVER_VERSION, EXPECTED_DATABASE_PRODUCT_VERSION);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.FLIGHT_SQL_SERVER_VERSION, flightSqlServerVersionProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlIdentifierQuoteCharProvider =
        (root, index) ->
            setDataForUtf8Field(root, index, SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR, EXPECTED_IDENTIFIER_QUOTE_STRING);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR, flightSqlIdentifierQuoteCharProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlServerReadOnlyProvider =
        (root, index) ->
            setDataForBooleanField(root, index, SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY, EXPECTED_IS_READ_ONLY);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY, flightSqlServerReadOnlyProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlKeywordsProvider =
        (root, index) -> setDataVarCharListField(root, index, SqlInfo.SQL_KEYWORDS,
            EXPECTED_SQL_KEYWORDS.split("\\s*,\\s*"));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_KEYWORDS, flightSqlKeywordsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlNumericFunctionsProvider =
        (root, index) -> setDataVarCharListField(root, index, SqlInfo.SQL_NUMERIC_FUNCTIONS,
            EXPECTED_NUMERIC_FUNCTIONS.split("\\s*,\\s*"));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_NUMERIC_FUNCTIONS, flightSqlNumericFunctionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlStringFunctionsProvider =
        (root, index) -> setDataVarCharListField(root, index, SqlInfo.SQL_STRING_FUNCTIONS,
            EXPECTED_STRING_FUNCTIONS.split("\\s*,\\s*"));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_STRING_FUNCTIONS, flightSqlStringFunctionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSystemFunctionsProvider =
        (root, index) -> setDataVarCharListField(root, index, SqlInfo.SQL_SYSTEM_FUNCTIONS,
            EXPECTED_SYSTEM_FUNCTIONS.split("\\s*,\\s*"));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SYSTEM_FUNCTIONS, flightSqlSystemFunctionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlTimeDateFunctionsProvider =
        (root, index) ->
            setDataVarCharListField(root, index, SqlInfo.SQL_DATETIME_FUNCTIONS,
                EXPECTED_TIME_DATE_FUNCTIONS.split("\\s*,\\s*"));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_DATETIME_FUNCTIONS, flightSqlTimeDateFunctionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSearchStringEscapeProvider =
        (root, index) ->
            setDataForUtf8Field(root, index, SqlInfo.SQL_SEARCH_STRING_ESCAPE, EXPECTED_SEARCH_STRING_ESCAPE);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SEARCH_STRING_ESCAPE, flightSqlSearchStringEscapeProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlExtraNamesCharacterProvider =
        (root, index) ->
            setDataForUtf8Field(root, index, SqlInfo.SQL_EXTRA_NAME_CHARACTERS, EXPECTED_EXTRA_NAME_CHARACTERS);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_EXTRA_NAME_CHARACTERS, flightSqlExtraNamesCharacterProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsColumnAliasingProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_COLUMN_ALIASING,
            EXPECTED_SUPPORTS_COLUMN_ALIASING);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_COLUMN_ALIASING, flightSqlSupportsColumnAliasingProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlNullPlusNullIsNullProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_NULL_PLUS_NULL_IS_NULL,
            EXPECTED_NULL_PLUS_NULL_IS_NULL);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_NULL_PLUS_NULL_IS_NULL, flightSqlNullPlusNullIsNullProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsConvertProvider =
        (root, index) -> setIntToIntListMapField(root, index, SqlInfo.SQL_SUPPORTS_CONVERT,
            SqlSupportsConvert.SQL_CONVERT_BIT_VALUE,
            new int[] {SqlSupportsConvert.SQL_CONVERT_INTEGER_VALUE, SqlSupportsConvert.SQL_CONVERT_BIGINT_VALUE});
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_CONVERT, flightSqlSupportsConvertProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsTableCorrelationNamesProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_TABLE_CORRELATION_NAMES,
            EXPECTED_SUPPORTS_TABLE_CORRELATION_NAMES);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_TABLE_CORRELATION_NAMES,
        flightSqlSupportsTableCorrelationNamesProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsDifferentTableCorrelationNamesProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES,
            EXPECTED_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES,
        flightSqlSupportsDifferentTableCorrelationNamesProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsExpressionsInOrderByProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY,
            EXPECTED_EXPRESSIONS_IN_ORDER_BY);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY,
        flightSqlSupportsExpressionsInOrderByProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsExpressionsInOrderByUnrelatedProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_ORDER_BY_UNRELATED,
            EXPECTED_SUPPORTS_ORDER_BY_UNRELATED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_ORDER_BY_UNRELATED,
        flightSqlSupportsExpressionsInOrderByUnrelatedProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedGroupByProvider =
        (root, index) -> setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_GROUP_BY,
            (int) (createBitmaskFromEnums(
                FlightSql.SqlSupportedGroupBy.SQL_GROUP_BY_UNRELATED)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_GROUP_BY, flightSqlSupportedGroupByProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsLikeEscapeClauseProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE,
            EXPECTED_SUPPORTS_LIKE_ESCAPE_CLAUSE);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE, flightSqlSupportsLikeEscapeClauseProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsNonNullableColumnsProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_NON_NULLABLE_COLUMNS,
            EXPECTED_NON_NULLABLE_COLUMNS);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_NON_NULLABLE_COLUMNS,
        flightSqlSupportsNonNullableColumnsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedGrammarProvider =
        (root, index) -> setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_GRAMMAR,
            (int) (createBitmaskFromEnums(SupportedSqlGrammar.SQL_CORE_GRAMMAR,
                SupportedSqlGrammar.SQL_MINIMUM_GRAMMAR)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_GRAMMAR, flightSqlSupportedGrammarProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlANSI92EntryLevelProvider =
        (root, index) -> setDataForIntField(root, index, SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
            (int) (createBitmaskFromEnums(SupportedAnsi92SqlGrammarLevel.ANSI92_ENTRY_SQL,
                SupportedAnsi92SqlGrammarLevel.ANSI92_INTERMEDIATE_SQL)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL, flightSqlANSI92EntryLevelProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsIntegrityEnhancementFacilityProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY,
            EXPECTED_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY,
        flightSqlSupportsIntegrityEnhancementFacilityProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSchemaTermProvider =
        (root, index) -> setDataForUtf8Field(root, index, SqlInfo.SQL_SCHEMA_TERM, EXPECTED_SCHEMA_TERM);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SCHEMA_TERM, flightSqlSchemaTermProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlCatalogTermProvider =
        (root, index) -> setDataForUtf8Field(root, index, SqlInfo.SQL_CATALOG_TERM, EXPECTED_CATALOG_TERM);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_CATALOG_TERM, flightSqlCatalogTermProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlProcedureTermProvider =
        (root, index) -> setDataForUtf8Field(root, index, SqlInfo.SQL_PROCEDURE_TERM, EXPECTED_PROCEDURE_TERM);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_PROCEDURE_TERM, flightSqlProcedureTermProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlCatalogAtStartProvider =
        (root, index) ->
            setDataForBooleanField(root, index, SqlInfo.SQL_CATALOG_AT_START, EXPECTED_CATALOG_AT_START);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_CATALOG_AT_START, flightSqlCatalogAtStartProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSchemasSupportedActionsProvider =
        (root, index) ->
            setDataForIntField(root, index, SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS,
                (int) (createBitmaskFromEnums(SqlSupportedElementActions.SQL_ELEMENT_IN_PROCEDURE_CALLS,
                    SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS, flightSqlSchemasSupportedActionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlCatalogSupportedActionsProvider =
        (root, index) ->
            setDataForIntField(root, index, SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS,
                (int) (createBitmaskFromEnums(SqlSupportedElementActions.SQL_ELEMENT_IN_INDEX_DEFINITIONS)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS, flightSqlCatalogSupportedActionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedPositionedCommandsProvider =
        (root, index) ->
            setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS,
                (int) (createBitmaskFromEnums(SqlSupportedPositionedCommands.SQL_POSITIONED_DELETE)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS,
        flightSqlSupportedPositionedCommandsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSelectForUpdateSupportedProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SELECT_FOR_UPDATE_SUPPORTED,
            EXPECTED_SELECT_FOR_UPDATE_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SELECT_FOR_UPDATE_SUPPORTED, flightSqlSelectForUpdateSupportedProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlStoredProceduresSupportedProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_STORED_PROCEDURES_SUPPORTED,
            EXPECTED_STORED_PROCEDURES_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_STORED_PROCEDURES_SUPPORTED, flightSqlStoredProceduresSupportedProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedSubqueriesProvider =
        (root, index) -> setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_SUBQUERIES,
            EXPECTED_SUPPORTED_SUBQUERIES);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_SUBQUERIES, flightSqlSupportedSubqueriesProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsCorrelatedSubqueriesProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED,
            EXPECTED_CORRELATED_SUBQUERIES_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED,
        flightSqlSupportsCorrelatedSubqueriesProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedUnionsLengthProvider =
        (root, index) -> setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_UNIONS,
            (int) (createBitmaskFromEnums(SqlSupportedUnions.SQL_UNION_ALL)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_UNIONS, flightSqlSupportedUnionsLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxBinaryLiteralLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_BINARY_LITERAL_LENGTH,
            EXPECTED_MAX_BINARY_LITERAL_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_BINARY_LITERAL_LENGTH, flightSqlMaxBinaryLiteralLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxCharLiteralLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_CHAR_LITERAL_LENGTH,
            EXPECTED_MAX_CHAR_LITERAL_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_CHAR_LITERAL_LENGTH, flightSqlMaxCharLiteralLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxColumnNameLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_COLUMN_NAME_LENGTH,
            EXPECTED_MAX_COLUMN_NAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_COLUMN_NAME_LENGTH, flightSqlMaxColumnNameLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxColumnsInGroupByProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_COLUMNS_IN_GROUP_BY,
            EXPECTED_MAX_COLUMNS_IN_GROUP_BY);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_COLUMNS_IN_GROUP_BY, flightSqlMaxColumnsInGroupByProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxColumnsInIndexProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_COLUMNS_IN_INDEX,
            EXPECTED_MAX_COLUMNS_IN_INDEX);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_COLUMNS_IN_INDEX, flightSqlMaxColumnsInIndexProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxColumnsInOrderByProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_COLUMNS_IN_ORDER_BY,
            EXPECTED_MAX_COLUMNS_IN_ORDER_BY);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_COLUMNS_IN_ORDER_BY, flightSqlMaxColumnsInOrderByProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxColumnsInSelectProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_COLUMNS_IN_SELECT,
            EXPECTED_MAX_COLUMNS_IN_SELECT);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_COLUMNS_IN_SELECT, flightSqlMaxColumnsInSelectProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxConnectionsProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_CONNECTIONS,
            EXPECTED_MAX_CONNECTIONS);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_CONNECTIONS, flightSqlMaxConnectionsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxCursorNameLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_CURSOR_NAME_LENGTH,
            EXPECTED_MAX_CURSOR_NAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_CURSOR_NAME_LENGTH, flightSqlMaxCursorNameLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxIndexLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_INDEX_LENGTH,
            EXPECTED_MAX_INDEX_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_INDEX_LENGTH, flightSqlMaxIndexLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxSchemaNameLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_SCHEMA_NAME_LENGTH,
            EXPECTED_SCHEMA_NAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SCHEMA_NAME_LENGTH, flightSqlMaxSchemaNameLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxSchemaProcedureLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_PROCEDURE_NAME_LENGTH,
            EXPECTED_MAX_PROCEDURE_NAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_PROCEDURE_NAME_LENGTH, flightSqlMaxSchemaProcedureLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxCatalogNameLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_CATALOG_NAME_LENGTH,
            EXPECTED_MAX_CATALOG_NAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_CATALOG_NAME_LENGTH, flightSqlMaxCatalogNameLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxRowSizeProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_ROW_SIZE,
            EXPECTED_MAX_ROW_SIZE);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_ROW_SIZE, flightSqlMaxRowSizeProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxRowSizeIncludeBlobsProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_MAX_ROW_SIZE_INCLUDES_BLOBS,
            EXPECTED_MAX_ROW_SIZE_INCLUDES_BLOBS);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_ROW_SIZE_INCLUDES_BLOBS, flightSqlMaxRowSizeIncludeBlobsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxStatementLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_STATEMENT_LENGTH,
            EXPECTED_MAX_STATEMENT_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_STATEMENT_LENGTH, flightSqlMaxStatementLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxStatementsProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_STATEMENTS,
            EXPECTED_MAX_STATEMENTS);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_STATEMENTS, flightSqlMaxStatementsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlTableNameLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_TABLE_NAME_LENGTH,
            EXPECTED_MAX_TABLE_NAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_TABLE_NAME_LENGTH, flightSqlTableNameLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlTablesInSelectProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_TABLES_IN_SELECT,
            EXPECTED_MAX_TABLES_IN_SELECT);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_TABLES_IN_SELECT, flightSqlTablesInSelectProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlMaxUsernameLengthProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_MAX_USERNAME_LENGTH,
            EXPECTED_MAX_USERNAME_LENGTH);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_MAX_USERNAME_LENGTH, flightSqlMaxUsernameLengthProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlDefaultTransactionIsolationProvider =
        (root, index) -> setDataForBigIntField(root, index, SqlInfo.SQL_DEFAULT_TRANSACTION_ISOLATION,
            EXPECTED_DEFAULT_TRANSACTION_ISOLATION);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_DEFAULT_TRANSACTION_ISOLATION,
        flightSqlDefaultTransactionIsolationProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlTransactionsSupportedProvider =
        (root, index) ->
            setDataForBooleanField(root, index, SqlInfo.SQL_TRANSACTIONS_SUPPORTED, EXPECTED_TRANSACTIONS_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_TRANSACTIONS_SUPPORTED, flightSqlTransactionsSupportedProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedTransactionsIsolationLevelsProvider =
        (root, index) ->
            setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS,
                (int) (createBitmaskFromEnums(SqlTransactionIsolationLevel.SQL_TRANSACTION_SERIALIZABLE,
                    SqlTransactionIsolationLevel.SQL_TRANSACTION_READ_COMMITTED)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS,
        flightSqlSupportedTransactionsIsolationLevelsProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlDataDefinitionCausesTransactionCommitProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT,
            EXPECTED_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT,
        flightSqlDataDefinitionCausesTransactionCommitProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlDataDefinitionInTransactionsIgnoredProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED,
            EXPECTED_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED,
        flightSqlDataDefinitionInTransactionsIgnoredProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportedResultSetTypesProvider =
        (root, index) -> setDataForIntField(root, index, SqlInfo.SQL_SUPPORTED_RESULT_SET_TYPES,
            (int) (createBitmaskFromEnums(
                SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY,
                SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE)));
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SUPPORTED_RESULT_SET_TYPES, flightSqlSupportedResultSetTypesProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSupportsBatchUpdatesProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_BATCH_UPDATES_SUPPORTED,
            EXPECTED_BATCH_UPDATES_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_BATCH_UPDATES_SUPPORTED, flightSqlSupportsBatchUpdatesProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlSavepointsSupportedProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_SAVEPOINTS_SUPPORTED,
            EXPECTED_SAVEPOINTS_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_SAVEPOINTS_SUPPORTED, flightSqlSavepointsSupportedProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlNamedParametersSupportedProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_NAMED_PARAMETERS_SUPPORTED,
            EXPECTED_NAMED_PARAMETERS_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_NAMED_PARAMETERS_SUPPORTED, flightSqlNamedParametersSupportedProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlLocatorsUpdateCopyProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_LOCATORS_UPDATE_COPY,
            EXPECTED_LOCATORS_UPDATE_COPY);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_LOCATORS_UPDATE_COPY, flightSqlLocatorsUpdateCopyProvider);

    final ObjIntConsumer<VectorSchemaRoot> flightSqlExpectedStoredFunctionsUsingCallSyntaxSupportedProvider =
        (root, index) -> setDataForBooleanField(root, index, SqlInfo.SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED,
            EXPECTED_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED);
    FLIGHT_SQL_PRODUCER.addSqlInfo(SqlInfo.SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED,
        flightSqlExpectedStoredFunctionsUsingCallSyntaxSupportedProvider);

    FLIGHT_SQL_PRODUCER.addDefaultSqlInfo(
        EnumSet.of(SqlInfo.FLIGHT_SQL_SERVER_NAME, SqlInfo.FLIGHT_SQL_SERVER_VERSION, SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
            SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY, SqlInfo.SQL_KEYWORDS, SqlInfo.SQL_NUMERIC_FUNCTIONS,
            SqlInfo.SQL_STRING_FUNCTIONS, SqlInfo.SQL_SYSTEM_FUNCTIONS, SqlInfo.SQL_DATETIME_FUNCTIONS,
            SqlInfo.SQL_SEARCH_STRING_ESCAPE, SqlInfo.SQL_EXTRA_NAME_CHARACTERS, SqlInfo.SQL_SUPPORTS_COLUMN_ALIASING,
            SqlInfo.SQL_NULL_PLUS_NULL_IS_NULL, SqlInfo.SQL_SUPPORTS_CONVERT,
            SqlInfo.SQL_SUPPORTS_TABLE_CORRELATION_NAMES, SqlInfo.SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES,
            SqlInfo.SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY, SqlInfo.SQL_SUPPORTS_ORDER_BY_UNRELATED,
            SqlInfo.SQL_SUPPORTED_GROUP_BY,
            SqlInfo.SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE, SqlInfo.SQL_SUPPORTS_NON_NULLABLE_COLUMNS,
            SqlInfo.SQL_SUPPORTED_GRAMMAR,
            SqlInfo.SQL_ANSI92_SUPPORTED_LEVEL,
            SqlInfo.SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY, SqlInfo.SQL_SCHEMA_TERM, SqlInfo.SQL_CATALOG_TERM,
            SqlInfo.SQL_PROCEDURE_TERM, SqlInfo.SQL_CATALOG_AT_START, SqlInfo.SQL_SCHEMAS_SUPPORTED_ACTIONS,
            SqlInfo.SQL_CATALOGS_SUPPORTED_ACTIONS, SqlInfo.SQL_SUPPORTED_POSITIONED_COMMANDS,
            SqlInfo.SQL_SELECT_FOR_UPDATE_SUPPORTED,
            SqlInfo.SQL_STORED_PROCEDURES_SUPPORTED, SqlInfo.SQL_SUPPORTED_SUBQUERIES,
            SqlInfo.SQL_CORRELATED_SUBQUERIES_SUPPORTED,
            SqlInfo.SQL_SUPPORTED_UNIONS, SqlInfo.SQL_MAX_BINARY_LITERAL_LENGTH, SqlInfo.SQL_MAX_CHAR_LITERAL_LENGTH,
            SqlInfo.SQL_MAX_COLUMN_NAME_LENGTH, SqlInfo.SQL_MAX_COLUMNS_IN_GROUP_BY, SqlInfo.SQL_MAX_COLUMNS_IN_INDEX,
            SqlInfo.SQL_MAX_COLUMNS_IN_ORDER_BY, SqlInfo.SQL_MAX_COLUMNS_IN_SELECT, SqlInfo.SQL_MAX_CONNECTIONS,
            SqlInfo.SQL_MAX_CURSOR_NAME_LENGTH, SqlInfo.SQL_MAX_INDEX_LENGTH, SqlInfo.SQL_SCHEMA_NAME_LENGTH,
            SqlInfo.SQL_MAX_PROCEDURE_NAME_LENGTH, SqlInfo.SQL_MAX_CATALOG_NAME_LENGTH,
            SqlInfo.SQL_MAX_ROW_SIZE, SqlInfo.SQL_MAX_ROW_SIZE_INCLUDES_BLOBS, SqlInfo.SQL_MAX_STATEMENT_LENGTH,
            SqlInfo.SQL_MAX_STATEMENTS, SqlInfo.SQL_MAX_TABLE_NAME_LENGTH, SqlInfo.SQL_MAX_TABLES_IN_SELECT,
            SqlInfo.SQL_MAX_USERNAME_LENGTH, SqlInfo.SQL_DEFAULT_TRANSACTION_ISOLATION,
            SqlInfo.SQL_TRANSACTIONS_SUPPORTED,
            SqlInfo.SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS, SqlInfo.SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT,
            SqlInfo.SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED, SqlInfo.SQL_SUPPORTED_RESULT_SET_TYPES,
            SqlInfo.SQL_BATCH_UPDATES_SUPPORTED, SqlInfo.SQL_SAVEPOINTS_SUPPORTED,
            SqlInfo.SQL_NAMED_PARAMETERS_SUPPORTED, SqlInfo.SQL_LOCATORS_UPDATE_COPY,
            SqlInfo.SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED));
    connection = FLIGHT_SERVER_TEST_RULE.getConnection();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    AutoCloseables.close(connection, FLIGHT_SERVER_TEST_RULE, FLIGHT_SQL_PRODUCER);
  }


  @Test
  public void testGetCatalogsCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_CATALOGS_RESULTS);
    }
  }

  @Test
  public void testGetCatalogsCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getCatalogs()) {
      resultSetTestUtils.testData(resultSet, singletonList("TABLE_CAT"), EXPECTED_GET_CATALOGS_RESULTS);
    }
  }

  @Test
  public void testTableTypesCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_TABLE_TYPES_RESULTS);
    }
  }

  @Test
  public void testTableTypesCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTableTypes()) {
      resultSetTestUtils.testData(resultSet, singletonList("TABLE_TYPE"), EXPECTED_GET_TABLE_TYPES_RESULTS);
    }
  }

  @Test
  public void testGetTablesCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_TABLES_RESULTS);
    }
  }

  @Test
  public void testGetTablesCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getTables(null, null, null, null)) {
      resultSetTestUtils.testData(
          resultSet,
          ImmutableList.of(
              "TABLE_CAT",
              "TABLE_SCHEM",
              "TABLE_NAME",
              "TABLE_TYPE",
              "REMARKS",
              "TYPE_CAT",
              "TYPE_SCHEM",
              "TYPE_NAME",
              "SELF_REFERENCING_COL_NAME",
              "REF_GENERATION"),
          EXPECTED_GET_TABLES_RESULTS
      );
    }
  }

  @Test
  public void testGetSchemasCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_SCHEMAS_RESULTS);
    }
  }

  @Test
  public void testGetSchemasCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getSchemas()) {
      resultSetTestUtils.testData(resultSet, ImmutableList.of("TABLE_SCHEM", "TABLE_CATALOG"),
          EXPECTED_GET_SCHEMAS_RESULTS);
    }
  }

  @Test
  public void testGetExportedKeysCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, TARGET_TABLE)) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS);
    }
  }

  @Test
  public void testGetExportedKeysCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getExportedKeys(null, null, TARGET_TABLE)) {
      resultSetTestUtils.testData(
          resultSet, FIELDS_GET_IMPORTED_EXPORTED_KEYS, EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS);
    }
  }

  @Test
  public void testGetImportedKeysCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, TARGET_TABLE)) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS);
    }
  }

  @Test
  public void testGetImportedKeysCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getImportedKeys(null, null, TARGET_TABLE)) {
      resultSetTestUtils.testData(
          resultSet, FIELDS_GET_IMPORTED_EXPORTED_KEYS, EXPECTED_GET_EXPORTED_AND_IMPORTED_KEYS_RESULTS);
    }
  }

  @Test
  public void testPrimaryKeysCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, TARGET_TABLE)) {
      resultSetTestUtils.testData(resultSet, EXPECTED_PRIMARY_KEYS_RESULTS);
    }
  }

  @Test
  public void testPrimaryKeysCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getPrimaryKeys(null, null, TARGET_TABLE)) {
      resultSetTestUtils.testData(
          resultSet,
          ImmutableList.of(
              "TABLE_CAT",
              "TABLE_SCHEM",
              "TABLE_NAME",
              "COLUMN_NAME",
              "KEY_SEQ",
              "PK_NAME"),
          EXPECTED_PRIMARY_KEYS_RESULTS
      );
    }
  }

  @Test
  public void testGetColumnsCanBeAccessedByIndices() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getColumns(null, null, null, null)) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_COLUMNS_RESULTS);
    }
  }

  @Test
  public void testGetColumnsCanByIndicesFilteringColumnNames() throws SQLException {
    char escapeChar = (char) 0;
    try (
        final ResultSet resultSet = connection.getMetaData()
            .getColumns(null, null, null, "column" + escapeChar + "_1")) {
      resultSetTestUtils.testData(resultSet, EXPECTED_GET_COLUMNS_RESULTS
          .stream()
          .filter(insideList -> Objects.equals(insideList.get(3), "column_1"))
          .collect(toList())
      );
    }
  }

  @Test
  public void testGetSqlInfo() throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    collector.checkThat(metaData.getDatabaseProductName(), is(EXPECTED_DATABASE_PRODUCT_NAME));
    collector.checkThat(metaData.getDatabaseProductVersion(), is(EXPECTED_DATABASE_PRODUCT_VERSION));
    collector.checkThat(metaData.getIdentifierQuoteString(), is(EXPECTED_IDENTIFIER_QUOTE_STRING));
    collector.checkThat(metaData.isReadOnly(), is(EXPECTED_IS_READ_ONLY));
    collector.checkThat(metaData.getSQLKeywords(), is(EXPECTED_SQL_KEYWORDS));
    collector.checkThat(metaData.getNumericFunctions(), is(EXPECTED_NUMERIC_FUNCTIONS));
    collector.checkThat(metaData.getStringFunctions(), is(EXPECTED_STRING_FUNCTIONS));
    collector.checkThat(metaData.getSystemFunctions(), is(EXPECTED_SYSTEM_FUNCTIONS));
    collector.checkThat(metaData.getTimeDateFunctions(), is(EXPECTED_TIME_DATE_FUNCTIONS));
    collector.checkThat(metaData.getSearchStringEscape(), is(EXPECTED_SEARCH_STRING_ESCAPE));
    collector.checkThat(metaData.getExtraNameCharacters(), is(EXPECTED_EXTRA_NAME_CHARACTERS));
    collector.checkThat(
        metaData.supportsConvert(BIT, INTEGER),
        is(EXPECTED_SQL_SUPPORTS_CONVERT));
    collector.checkThat(
        metaData.supportsConvert(BIT, BIGINT),
        is(EXPECTED_SQL_SUPPORTS_CONVERT));
    collector.checkThat(
        metaData.supportsConvert(BIGINT, INTEGER),
        is(EXPECTED_INVALID_SQL_SUPPORTS_CONVERT));
    collector.checkThat(
        metaData.supportsConvert(JAVA_OBJECT, INTEGER),
        is(EXPECTED_INVALID_SQL_SUPPORTS_CONVERT));
    collector.checkThat(metaData.supportsTableCorrelationNames(), is(EXPECTED_SUPPORTS_TABLE_CORRELATION_NAMES));
    collector.checkThat(metaData.supportsDifferentTableCorrelationNames(),
        is(EXPECTED_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES));
    collector.checkThat(metaData.supportsExpressionsInOrderBy(), is(EXPECTED_EXPRESSIONS_IN_ORDER_BY));
    collector.checkThat(metaData.supportsOrderByUnrelated(), is(EXPECTED_SUPPORTS_ORDER_BY_UNRELATED));
    collector.checkThat(metaData.supportsGroupBy(), is(EXPECTED_SUPPORTS_GROUP_BY));
    collector.checkThat(metaData.supportsGroupByUnrelated(), is(EXPECTED_SUPPORTS_GROUP_BY_UNRELATED));
    collector.checkThat(metaData.supportsLikeEscapeClause(), is(EXPECTED_SUPPORTS_LIKE_ESCAPE_CLAUSE));
    collector.checkThat(metaData.supportsNonNullableColumns(), is(EXPECTED_NON_NULLABLE_COLUMNS));
    collector.checkThat(metaData.supportsMinimumSQLGrammar(), is(EXPECTED_MINIMUM_SQL_GRAMMAR));
    collector.checkThat(metaData.supportsCoreSQLGrammar(), is(EXPECTED_CORE_SQL_GRAMMAR));
    collector.checkThat(metaData.supportsExtendedSQLGrammar(), is(EXPECTED_EXTEND_SQL_GRAMMAR));
    collector.checkThat(metaData.supportsANSI92EntryLevelSQL(), is(EXPECTED_ANSI92_ENTRY_LEVEL_SQL));
    collector.checkThat(metaData.supportsANSI92IntermediateSQL(), is(EXPECTED_ANSI92_INTERMEDIATE_SQL));
    collector.checkThat(metaData.supportsANSI92FullSQL(), is(EXPECTED_ANSI92_FULL_SQL));
    collector.checkThat(metaData.supportsIntegrityEnhancementFacility(),
        is(EXPECTED_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY));
    collector.checkThat(metaData.getSchemaTerm(), is(EXPECTED_SCHEMA_TERM));
    collector.checkThat(metaData.getProcedureTerm(), is(EXPECTED_PROCEDURE_TERM));
    collector.checkThat(metaData.getCatalogTerm(), is(EXPECTED_CATALOG_TERM));
    collector.checkThat(metaData.isCatalogAtStart(), is(EXPECTED_CATALOG_AT_START));
    collector.checkThat(metaData.supportsSchemasInProcedureCalls(), is(EXPECTED_SCHEMAS_IN_PROCEDURE_CALLS));
    collector.checkThat(metaData.supportsSchemasInIndexDefinitions(), is(EXPECTED_SCHEMAS_IN_INDEX_DEFINITIONS));
    collector.checkThat(metaData.supportsSchemasInPrivilegeDefinitions(),
        is(EXPECTED_SCHEMAS_IN_PRIVILEGE_DEFINITIONS));
    collector.checkThat(metaData.supportsCatalogsInIndexDefinitions(), is(EXPECTED_CATALOGS_IN_INDEX_DEFINITIONS));
    collector.checkThat(metaData.supportsCatalogsInPrivilegeDefinitions(),
        is(EXPECTED_CATALOGS_IN_PRIVILEGE_DEFINITIONS));
    collector.checkThat(metaData.supportsPositionedDelete(), is(EXPECTED_POSITIONED_DELETE));
    collector.checkThat(metaData.supportsPositionedUpdate(), is(EXPECTED_POSITIONED_UPDATE));
    collector.checkThat(metaData.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY), is(EXPECTED_TYPE_FORWARD_ONLY));
    collector.checkThat(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE),
        is(EXPECTED_TYPE_SCROLL_INSENSITIVE));
    collector.checkThat(metaData.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE),
        is(EXPECTED_TYPE_SCROLL_SENSITIVE));
    collector.checkThrows(SQLException.class, () -> metaData.supportsResultSetType(ResultSet.HOLD_CURSORS_OVER_COMMIT));
    collector.checkThat(metaData.supportsSelectForUpdate(), is(EXPECTED_SELECT_FOR_UPDATE_SUPPORTED));
    collector.checkThat(metaData.supportsStoredProcedures(), is(EXPECTED_STORED_PROCEDURES_SUPPORTED));
    collector.checkThat(metaData.supportsSubqueriesInComparisons(), is(EXPECTED_SUBQUERIES_IN_COMPARISON));
    collector.checkThat(metaData.supportsSubqueriesInExists(), is(EXPECTED_SUBQUERIES_IN_EXISTS));
    collector.checkThat(metaData.supportsSubqueriesInIns(), is(EXPECTED_SUBQUERIES_IN_INS));
    collector.checkThat(metaData.supportsSubqueriesInQuantifieds(), is(EXPECTED_SUBQUERIES_IN_QUANTIFIEDS));
    collector.checkThat(metaData.supportsCorrelatedSubqueries(), is(EXPECTED_CORRELATED_SUBQUERIES_SUPPORTED));
    collector.checkThat(metaData.supportsUnion(), is(EXPECTED_SUPPORTS_UNION));
    collector.checkThat(metaData.supportsUnionAll(), is(EXPECTED_SUPPORTS_UNION_ALL));
    collector.checkThat(metaData.getMaxBinaryLiteralLength(), is(EXPECTED_MAX_BINARY_LITERAL_LENGTH));
    collector.checkThat(metaData.getMaxCharLiteralLength(), is(EXPECTED_MAX_CHAR_LITERAL_LENGTH));
    collector.checkThat(metaData.getMaxColumnsInGroupBy(), is(EXPECTED_MAX_COLUMNS_IN_GROUP_BY));
    collector.checkThat(metaData.getMaxColumnsInIndex(), is(EXPECTED_MAX_COLUMNS_IN_INDEX));
    collector.checkThat(metaData.getMaxColumnsInOrderBy(), is(EXPECTED_MAX_COLUMNS_IN_ORDER_BY));
    collector.checkThat(metaData.getMaxColumnsInSelect(), is(EXPECTED_MAX_COLUMNS_IN_SELECT));
    collector.checkThat(metaData.getMaxConnections(), is(EXPECTED_MAX_CONNECTIONS));
    collector.checkThat(metaData.getMaxCursorNameLength(), is(EXPECTED_MAX_CURSOR_NAME_LENGTH));
    collector.checkThat(metaData.getMaxIndexLength(), is(EXPECTED_MAX_INDEX_LENGTH));
    collector.checkThat(metaData.getMaxSchemaNameLength(), is(EXPECTED_SCHEMA_NAME_LENGTH));
    collector.checkThat(metaData.getMaxProcedureNameLength(), is(EXPECTED_MAX_PROCEDURE_NAME_LENGTH));
    collector.checkThat(metaData.getMaxCatalogNameLength(), is(EXPECTED_MAX_CATALOG_NAME_LENGTH));
    collector.checkThat(metaData.getMaxRowSize(), is(EXPECTED_MAX_ROW_SIZE));
    collector.checkThat(metaData.doesMaxRowSizeIncludeBlobs(), is(EXPECTED_MAX_ROW_SIZE_INCLUDES_BLOBS));
    collector.checkThat(metaData.getMaxStatementLength(), is(EXPECTED_MAX_STATEMENT_LENGTH));
    collector.checkThat(metaData.getMaxStatements(), is(EXPECTED_MAX_STATEMENTS));
    collector.checkThat(metaData.getMaxTableNameLength(), is(EXPECTED_MAX_TABLE_NAME_LENGTH));
    collector.checkThat(metaData.getMaxTablesInSelect(), is(EXPECTED_MAX_TABLES_IN_SELECT));
    collector.checkThat(metaData.getMaxUserNameLength(), is(EXPECTED_MAX_USERNAME_LENGTH));
    collector.checkThat(metaData.getDefaultTransactionIsolation(), is(EXPECTED_DEFAULT_TRANSACTION_ISOLATION));
    collector.checkThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_NONE),
        is(EXPECTED_TRANSACTION_NONE));
    collector.checkThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED),
        is(EXPECTED_TRANSACTION_READ_COMMITTED));
    collector.checkThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED),
        is(EXPECTED_TRANSACTION_READ_UNCOMMITTED));
    collector.checkThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ),
        is(EXPECTED_TRANSACTION_REPEATABLE_READ));
    collector.checkThat(metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE),
        is(EXPECTED_TRANSACTION_SERIALIZABLE));
    collector.checkThrows(SQLException.class,
        () -> metaData.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE + 1));
    collector.checkThat(metaData.supportsTransactions(), is(EXPECTED_TRANSACTIONS_SUPPORTED));
    collector.checkThat(metaData.supportsSubqueriesInComparisons(), is(EXPECTED_SUBQUERIES_IN_COMPARISON));
    collector.checkThat(metaData.dataDefinitionCausesTransactionCommit(),
        is(EXPECTED_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT));
    collector.checkThat(metaData.dataDefinitionIgnoredInTransactions(),
        is(EXPECTED_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED));
    collector.checkThat(metaData.supportsBatchUpdates(), is(EXPECTED_BATCH_UPDATES_SUPPORTED));
    collector.checkThat(metaData.supportsSavepoints(), is(EXPECTED_SAVEPOINTS_SUPPORTED));
    collector.checkThat(metaData.supportsNamedParameters(), is(EXPECTED_NAMED_PARAMETERS_SUPPORTED));
    collector.checkThat(metaData.locatorsUpdateCopy(), is(EXPECTED_LOCATORS_UPDATE_COPY));
    collector.checkThat(metaData.supportsStoredFunctionsUsingCallSyntax(),
        is(EXPECTED_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED));
  }

  @Test
  public void testGetColumnsCanBeAccessedByNames() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getColumns(null, null, null, null)) {
      resultSetTestUtils.testData(resultSet,
          ImmutableList.of(
              "TABLE_CAT",
              "TABLE_SCHEM",
              "TABLE_NAME",
              "COLUMN_NAME",
              "DATA_TYPE",
              "TYPE_NAME",
              "COLUMN_SIZE",
              "BUFFER_LENGTH",
              "DECIMAL_DIGITS",
              "NUM_PREC_RADIX",
              "NULLABLE",
              "REMARKS",
              "COLUMN_DEF",
              "SQL_DATA_TYPE",
              "SQL_DATETIME_SUB",
              "CHAR_OCTET_LENGTH",
              "ORDINAL_POSITION",
              "IS_NULLABLE",
              "SCOPE_CATALOG",
              "SCOPE_SCHEMA",
              "SCOPE_TABLE",
              "SOURCE_DATA_TYPE",
              "IS_AUTOINCREMENT",
              "IS_GENERATEDCOLUMN"),
          EXPECTED_GET_COLUMNS_RESULTS
      );
    }
  }

  @Test
  public void testGetProcedures() throws SQLException {
    try (final ResultSet resultSet = connection.getMetaData().getProcedures(null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetProceduresSchema = new HashMap<Integer, String>() {
        {
          put(1, "PROCEDURE_CAT");
          put(2, "PROCEDURE_SCHEM");
          put(3, "PROCEDURE_NAME");
          put(4, "FUTURE_USE1");
          put(5, "FUTURE_USE2");
          put(6, "FUTURE_USE3");
          put(7, "REMARKS");
          put(8, "PROCEDURE_TYPE");
          put(9, "SPECIFIC_NAME");
        }
      };
      testEmptyResultSet(resultSet, expectedGetProceduresSchema);
    }
  }

  @Test
  public void testGetProcedureColumns() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getProcedureColumns(null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetProcedureColumnsSchema = new HashMap<Integer, String>() {
        {
          put(1, "PROCEDURE_CAT");
          put(2, "PROCEDURE_SCHEM");
          put(3, "PROCEDURE_NAME");
          put(4, "COLUMN_NAME");
          put(5, "COLUMN_TYPE");
          put(6, "DATA_TYPE");
          put(7, "TYPE_NAME");
          put(8, "PRECISION");
          put(9, "LENGTH");
          put(10, "SCALE");
          put(11, "RADIX");
          put(12, "NULLABLE");
          put(13, "REMARKS");
          put(14, "COLUMN_DEF");
          put(15, "SQL_DATA_TYPE");
          put(16, "SQL_DATETIME_SUB");
          put(17, "CHAR_OCTET_LENGTH");
          put(18, "ORDINAL_POSITION");
          put(19, "IS_NULLABLE");
          put(20, "SPECIFIC_NAME");
        }
      };
      testEmptyResultSet(resultSet, expectedGetProcedureColumnsSchema);
    }
  }

  @Test
  public void testGetColumnPrivileges() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getColumnPrivileges(null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetColumnPrivilegesSchema = new HashMap<Integer, String>() {
        {
          put(1, "TABLE_CAT");
          put(2, "TABLE_SCHEM");
          put(3, "TABLE_NAME");
          put(4, "COLUMN_NAME");
          put(5, "GRANTOR");
          put(6, "GRANTEE");
          put(7, "PRIVILEGE");
          put(8, "IS_GRANTABLE");
        }
      };
      testEmptyResultSet(resultSet, expectedGetColumnPrivilegesSchema);
    }
  }

  @Test
  public void testGetTablePrivileges() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getTablePrivileges(null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetTablePrivilegesSchema = new HashMap<Integer, String>() {
        {
          put(1, "TABLE_CAT");
          put(2, "TABLE_SCHEM");
          put(3, "TABLE_NAME");
          put(4, "GRANTOR");
          put(5, "GRANTEE");
          put(6, "PRIVILEGE");
          put(7, "IS_GRANTABLE");
        }
      };
      testEmptyResultSet(resultSet, expectedGetTablePrivilegesSchema);
    }
  }

  @Test
  public void testGetBestRowIdentifier() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getBestRowIdentifier(null, null, null, 0, true)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetBestRowIdentifierSchema = new HashMap<Integer, String>() {
        {
          put(1, "SCOPE");
          put(2, "COLUMN_NAME");
          put(3, "DATA_TYPE");
          put(4, "TYPE_NAME");
          put(5, "COLUMN_SIZE");
          put(6, "BUFFER_LENGTH");
          put(7, "DECIMAL_DIGITS");
          put(8, "PSEUDO_COLUMN");
        }
      };
      testEmptyResultSet(resultSet, expectedGetBestRowIdentifierSchema);
    }
  }

  @Test
  public void testGetVersionColumns() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getVersionColumns(null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetVersionColumnsSchema = new HashMap<Integer, String>() {
        {
          put(1, "SCOPE");
          put(2, "COLUMN_NAME");
          put(3, "DATA_TYPE");
          put(4, "TYPE_NAME");
          put(5, "COLUMN_SIZE");
          put(6, "BUFFER_LENGTH");
          put(7, "DECIMAL_DIGITS");
          put(8, "PSEUDO_COLUMN");
        }
      };
      testEmptyResultSet(resultSet, expectedGetVersionColumnsSchema);
    }
  }

  @Test
  public void testGetCrossReference() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getCrossReference(null, null, null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetCrossReferenceSchema = new HashMap<Integer, String>() {
        {
          put(1, "PKTABLE_CAT");
          put(2, "PKTABLE_SCHEM");
          put(3, "PKTABLE_NAME");
          put(4, "PKCOLUMN_NAME");
          put(5, "FKTABLE_CAT");
          put(6, "FKTABLE_SCHEM");
          put(7, "FKTABLE_NAME");
          put(8, "FKCOLUMN_NAME");
          put(9, "KEY_SEQ");
          put(10, "UPDATE_RULE");
          put(11, "DELETE_RULE");
          put(12, "FK_NAME");
          put(13, "PK_NAME");
          put(14, "DEFERABILITY");
        }
      };
      testEmptyResultSet(resultSet, expectedGetCrossReferenceSchema);
    }
  }

  @Test
  public void testGetTypeInfo() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getTypeInfo()) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetTypeInfoSchema = new HashMap<Integer, String>() {
        {
          put(1, "TYPE_NAME");
          put(2, "DATA_TYPE");
          put(3, "PRECISION");
          put(4, "LITERAL_PREFIX");
          put(5, "LITERAL_SUFFIX");
          put(6, "CREATE_PARAMS");
          put(7, "NULLABLE");
          put(8, "CASE_SENSITIVE");
          put(9, "SEARCHABLE");
          put(10, "UNSIGNED_ATTRIBUTE");
          put(11, "FIXED_PREC_SCALE");
          put(12, "AUTO_INCREMENT");
          put(13, "LOCAL_TYPE_NAME");
          put(14, "MINIMUM_SCALE");
          put(15, "MAXIMUM_SCALE");
          put(16, "SQL_DATA_TYPE");
          put(17, "SQL_DATETIME_SUB");
          put(18, "NUM_PREC_RADIX");
        }
      };
      testEmptyResultSet(resultSet, expectedGetTypeInfoSchema);
    }
  }

  @Test
  public void testGetIndexInfo() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getIndexInfo(null, null, null, false, true)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetIndexInfoSchema = new HashMap<Integer, String>() {
        {
          put(1, "TABLE_CAT");
          put(2, "TABLE_SCHEM");
          put(3, "TABLE_NAME");
          put(4, "NON_UNIQUE");
          put(5, "INDEX_QUALIFIER");
          put(6, "INDEX_NAME");
          put(7, "TYPE");
          put(8, "ORDINAL_POSITION");
          put(9, "COLUMN_NAME");
          put(10, "ASC_OR_DESC");
          put(11, "CARDINALITY");
          put(12, "PAGES");
          put(13, "FILTER_CONDITION");
        }
      };
      testEmptyResultSet(resultSet, expectedGetIndexInfoSchema);
    }
  }

  @Test
  public void testGetUDTs() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getUDTs(null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetUDTsSchema = new HashMap<Integer, String>() {
        {
          put(1, "TYPE_CAT");
          put(2, "TYPE_SCHEM");
          put(3, "TYPE_NAME");
          put(4, "CLASS_NAME");
          put(5, "DATA_TYPE");
          put(6, "REMARKS");
          put(7, "BASE_TYPE");
        }
      };
      testEmptyResultSet(resultSet, expectedGetUDTsSchema);
    }
  }

  @Test
  public void testGetSuperTypes() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getSuperTypes(null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetSuperTypesSchema = new HashMap<Integer, String>() {
        {
          put(1, "TYPE_CAT");
          put(2, "TYPE_SCHEM");
          put(3, "TYPE_NAME");
          put(4, "SUPERTYPE_CAT");
          put(5, "SUPERTYPE_SCHEM");
          put(6, "SUPERTYPE_NAME");
        }
      };
      testEmptyResultSet(resultSet, expectedGetSuperTypesSchema);
    }
  }

  @Test
  public void testGetSuperTables() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getSuperTables(null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetSuperTablesSchema = new HashMap<Integer, String>() {
        {
          put(1, "TABLE_CAT");
          put(2, "TABLE_SCHEM");
          put(3, "TABLE_NAME");
          put(4, "SUPERTABLE_NAME");
        }
      };
      testEmptyResultSet(resultSet, expectedGetSuperTablesSchema);
    }
  }

  @Test
  public void testGetAttributes() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getAttributes(null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetAttributesSchema = new HashMap<Integer, String>() {
        {
          put(1, "TYPE_CAT");
          put(2, "TYPE_SCHEM");
          put(3, "TYPE_NAME");
          put(4, "ATTR_NAME");
          put(5, "DATA_TYPE");
          put(6, "ATTR_TYPE_NAME");
          put(7, "ATTR_SIZE");
          put(8, "DECIMAL_DIGITS");
          put(9, "NUM_PREC_RADIX");
          put(10, "NULLABLE");
          put(11, "REMARKS");
          put(12, "ATTR_DEF");
          put(13, "SQL_DATA_TYPE");
          put(14, "SQL_DATETIME_SUB");
          put(15, "CHAR_OCTET_LENGTH");
          put(16, "ORDINAL_POSITION");
          put(17, "IS_NULLABLE");
          put(18, "SCOPE_CATALOG");
          put(19, "SCOPE_SCHEMA");
          put(20, "SCOPE_TABLE");
          put(21, "SOURCE_DATA_TYPE");
        }
      };
      testEmptyResultSet(resultSet, expectedGetAttributesSchema);
    }
  }

  @Test
  public void testGetClientInfoProperties() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getClientInfoProperties()) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetClientInfoPropertiesSchema = new HashMap<Integer, String>() {
        {
          put(1, "NAME");
          put(2, "MAX_LEN");
          put(3, "DEFAULT_VALUE");
          put(4, "DESCRIPTION");
        }
      };
      testEmptyResultSet(resultSet, expectedGetClientInfoPropertiesSchema);
    }
  }

  @Test
  public void testGetFunctions() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getFunctions(null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetFunctionsSchema = new HashMap<Integer, String>() {
        {
          put(1, "FUNCTION_CAT");
          put(2, "FUNCTION_SCHEM");
          put(3, "FUNCTION_NAME");
          put(4, "REMARKS");
          put(5, "FUNCTION_TYPE");
          put(6, "SPECIFIC_NAME");
        }
      };
      testEmptyResultSet(resultSet, expectedGetFunctionsSchema);
    }
  }

  @Test
  public void testGetFunctionColumns() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getFunctionColumns(null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetFunctionColumnsSchema = new HashMap<Integer, String>() {
        {
          put(1, "FUNCTION_CAT");
          put(2, "FUNCTION_SCHEM");
          put(3, "FUNCTION_NAME");
          put(4, "COLUMN_NAME");
          put(5, "COLUMN_TYPE");
          put(6, "DATA_TYPE");
          put(7, "TYPE_NAME");
          put(8, "PRECISION");
          put(9, "LENGTH");
          put(10, "SCALE");
          put(11, "RADIX");
          put(12, "NULLABLE");
          put(13, "REMARKS");
          put(14, "CHAR_OCTET_LENGTH");
          put(15, "ORDINAL_POSITION");
          put(16, "IS_NULLABLE");
          put(17, "SPECIFIC_NAME");
        }
      };
      testEmptyResultSet(resultSet, expectedGetFunctionColumnsSchema);
    }
  }

  @Test
  public void testGetPseudoColumns() throws SQLException {
    try (ResultSet resultSet = connection.getMetaData().getPseudoColumns(null, null, null, null)) {
      // Maps ordinal index to column name according to JDBC documentation
      final Map<Integer, String> expectedGetPseudoColumnsSchema = new HashMap<Integer, String>() {
        {
          put(1, "TABLE_CAT");
          put(2, "TABLE_SCHEM");
          put(3, "TABLE_NAME");
          put(4, "COLUMN_NAME");
          put(5, "DATA_TYPE");
          put(6, "COLUMN_SIZE");
          put(7, "DECIMAL_DIGITS");
          put(8, "NUM_PREC_RADIX");
          put(9, "COLUMN_USAGE");
          put(10, "REMARKS");
          put(11, "CHAR_OCTET_LENGTH");
          put(12, "IS_NULLABLE");
        }
      };
      testEmptyResultSet(resultSet, expectedGetPseudoColumnsSchema);
    }
  }

  private void testEmptyResultSet(final ResultSet resultSet, final Map<Integer, String> expectedResultSetSchema)
      throws SQLException {
    Assert.assertFalse(resultSet.next());
    final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    for (final Map.Entry<Integer, String> entry : expectedResultSetSchema.entrySet()) {
      Assert.assertEquals(entry.getValue(), resultSetMetaData.getColumnLabel(entry.getKey()));
    }
  }

  @Test
  public void testGetColumnSize() {
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_BYTE),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Int(Byte.SIZE, true)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_SHORT),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Int(Short.SIZE, true)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_INT),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Int(Integer.SIZE, true)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_LONG),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Int(Long.SIZE, true)));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_VARCHAR_AND_BINARY),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Utf8()));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_VARCHAR_AND_BINARY),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Binary()));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIMESTAMP_SECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Timestamp(TimeUnit.SECOND, null)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIMESTAMP_MILLISECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIMESTAMP_MICROSECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIMESTAMP_NANOSECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIME),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Time(TimeUnit.SECOND, Integer.SIZE)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIME_MILLISECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Time(TimeUnit.MILLISECOND, Integer.SIZE)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIME_MICROSECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Time(TimeUnit.MICROSECOND, Integer.SIZE)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_TIME_NANOSECONDS),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Time(TimeUnit.NANOSECOND, Integer.SIZE)));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.COLUMN_SIZE_DATE),
        ArrowDatabaseMetadata.getColumnSize(new ArrowType.Date(DateUnit.DAY)));

    Assert.assertNull(ArrowDatabaseMetadata.getColumnSize(new ArrowType.FloatingPoint(
        FloatingPointPrecision.DOUBLE)));
  }

  @Test
  public void testGetDecimalDigits() {
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.NO_DECIMAL_DIGITS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Int(Byte.SIZE, true)));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.NO_DECIMAL_DIGITS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Timestamp(TimeUnit.SECOND, null)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.DECIMAL_DIGITS_TIME_MILLISECONDS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.DECIMAL_DIGITS_TIME_MICROSECONDS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.DECIMAL_DIGITS_TIME_NANOSECONDS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.NO_DECIMAL_DIGITS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Time(TimeUnit.SECOND, Integer.SIZE)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.DECIMAL_DIGITS_TIME_MILLISECONDS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Time(TimeUnit.MILLISECOND, Integer.SIZE)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.DECIMAL_DIGITS_TIME_MICROSECONDS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Time(TimeUnit.MICROSECOND, Integer.SIZE)));
    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.DECIMAL_DIGITS_TIME_NANOSECONDS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Time(TimeUnit.NANOSECOND, Integer.SIZE)));

    Assert.assertEquals(Integer.valueOf(ArrowDatabaseMetadata.NO_DECIMAL_DIGITS),
        ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Date(DateUnit.DAY)));

    Assert.assertNull(ArrowDatabaseMetadata.getDecimalDigits(new ArrowType.Utf8()));
  }

  @Test
  public void testSqlToRegexLike() {
    Assert.assertEquals(".*", ArrowDatabaseMetadata.sqlToRegexLike("%"));
    Assert.assertEquals(".", ArrowDatabaseMetadata.sqlToRegexLike("_"));
    Assert.assertEquals("\\*", ArrowDatabaseMetadata.sqlToRegexLike("*"));
    Assert.assertEquals("T\\*E.S.*T", ArrowDatabaseMetadata.sqlToRegexLike("T*E_S%T"));
  }
}

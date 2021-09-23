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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.EnumMap;
import java.util.Map;

import org.apache.arrow.driver.jdbc.utils.SqlTypes;
import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

/**
 * Arrow Flight JDBC's implementation of {@link DatabaseMetaData}.
 */
public class ArrowDatabaseMetadata extends AvaticaDatabaseMetaData {
  private static final String JAVA_REGEX_SPECIALS = "[]()|^-+*?{}$\\.";
  private static final Charset CHARSET = StandardCharsets.UTF_8;
  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  private static final int NO_DECIMAL_DIGITS = 0;
  private static final int BASE10_RADIX = 10;
  private static final int COLUMN_SIZE_BYTE = (int) Math.ceil((Byte.SIZE - 1) * Math.log(2) / Math.log(10));
  private static final int COLUMN_SIZE_SHORT = (int) Math.ceil((Short.SIZE - 1) * Math.log(2) / Math.log(10));
  private static final int COLUMN_SIZE_INT = (int) Math.ceil((Integer.SIZE - 1) * Math.log(2) / Math.log(10));
  private static final int COLUMN_SIZE_LONG = (int) Math.ceil((Long.SIZE - 1) * Math.log(2) / Math.log(10));
  private static final int COLUMN_SIZE_VARCHAR_AND_BINARY = 65536;
  private static final int COLUMN_SIZE_DATE = "YYYY-MM-DD".length();
  private static final int COLUMN_SIZE_TIME = "HH:MM:ss".length();
  private static final int COLUMN_SIZE_TIME_MILLISECONDS = "HH:MM:ss.SSS".length();
  private static final int COLUMN_SIZE_TIME_MICROSECONDS = "HH:MM:ss.SSSSSS".length();
  private static final int COLUMN_SIZE_TIME_NANOSECONDS = "HH:MM:ss.SSSSSSSSS".length();
  private static final int COLUMN_SIZE_TIMESTAMP_SECONDS = COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME;
  private static final int COLUMN_SIZE_TIMESTAMP_MILLISECONDS = COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME_MILLISECONDS;
  private static final int COLUMN_SIZE_TIMESTAMP_MICROSECONDS = COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME_MICROSECONDS;
  private static final int COLUMN_SIZE_TIMESTAMP_NANOSECONDS = COLUMN_SIZE_DATE + 1 + COLUMN_SIZE_TIME_NANOSECONDS;
  private static final int DECIMAL_DIGITS_TIME_MILLISECONDS = 3;
  private static final int DECIMAL_DIGITS_TIME_MICROSECONDS = 6;
  private static final int DECIMAL_DIGITS_TIME_NANOSECONDS = 9;
  private static final Schema GET_COLUMNS_SCHEMA = new Schema(
      Arrays.asList(
          Field.nullable("TABLE_CAT", Types.MinorType.VARCHAR.getType()),
          Field.nullable("TABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("TABLE_NAME", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("COLUMN_NAME", Types.MinorType.VARCHAR.getType()),
          Field.nullable("DATA_TYPE", Types.MinorType.INT.getType()),
          Field.nullable("TYPE_NAME", Types.MinorType.VARCHAR.getType()),
          Field.nullable("COLUMN_SIZE", Types.MinorType.INT.getType()),
          Field.nullable("BUFFER_LENGTH", Types.MinorType.INT.getType()),
          Field.nullable("DECIMAL_DIGITS", Types.MinorType.INT.getType()),
          Field.nullable("NUM_PREC_RADIX", Types.MinorType.INT.getType()),
          Field.notNullable("NULLABLE", Types.MinorType.INT.getType()),
          Field.nullable("REMARKS", Types.MinorType.VARCHAR.getType()),
          Field.nullable("COLUMN_DEF", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SQL_DATA_TYPE", Types.MinorType.INT.getType()),
          Field.nullable("SQL_DATETIME_SUB", Types.MinorType.INT.getType()),
          Field.notNullable("CHAR_OCTET_LENGTH", Types.MinorType.INT.getType()),
          Field.notNullable("ORDINAL_POSITION", Types.MinorType.INT.getType()),
          Field.notNullable("IS_NULLABLE", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SCOPE_CATALOG", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SCOPE_SCHEMA", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SCOPE_TABLE", Types.MinorType.VARCHAR.getType()),
          Field.nullable("SOURCE_DATA_TYPE", Types.MinorType.SMALLINT.getType()),
          Field.notNullable("IS_AUTOINCREMENT", Types.MinorType.VARCHAR.getType()),
          Field.notNullable("IS_GENERATEDCOLUMN", Types.MinorType.VARCHAR.getType())
      ));
  private final Map<SqlInfo, Object> cachedSqlInfo = new EnumMap<>(SqlInfo.class);

  ArrowDatabaseMetadata(final AvaticaConnection connection) {
    super(connection);
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.FLIGHT_SQL_SERVER_NAME, String.class);
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.FLIGHT_SQL_SERVER_VERSION, String.class);
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR, String.class);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return getSqlInfoAndCacheIfCacheIsEmpty(SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY, Integer.class) == 1;
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  private synchronized <T> T getSqlInfoAndCacheIfCacheIsEmpty(final SqlInfo sqlInfoCommand, final Class<T> desiredType)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo sqlInfo = connection.getClientHandler().getSqlInfo();
    if (cachedSqlInfo.isEmpty()) {
      try (final ResultSet resultSet =
               ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(
                   connection, sqlInfo, null)) {
        while (resultSet.next()) {
          cachedSqlInfo.put(SqlInfo.forNumber((Integer) resultSet.getObject("info_name")),
              resultSet.getObject("value"));
        }
      }
    }
    return desiredType.cast(cachedSqlInfo.get(sqlInfoCommand));
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoCatalogs = connection.getClientHandler().getCatalogs();

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_CATALOGS_SCHEMA, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoCatalogs, transformer);
  }

  @Override
  public ResultSet getImportedKeys(final String catalog, final String schema, final String table) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoImportedKeys = connection.getClientHandler().getImportedKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer = getImportedExportedKeysTransformer(allocator);
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoImportedKeys, transformer);
  }

  @Override
  public ResultSet getExportedKeys(final String catalog, final String schema, final String table) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoExportedKeys = connection.getClientHandler().getExportedKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer = getImportedExportedKeysTransformer(allocator);
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoExportedKeys, transformer);
  }

  private VectorSchemaRootTransformer getImportedExportedKeysTransformer(final BufferAllocator allocator) {
    return new VectorSchemaRootTransformer.Builder(Schemas.GET_IMPORTED_AND_EXPORTED_KEYS_SCHEMA,
        allocator)
        .renameFieldVector("pk_catalog_name", "PKTABLE_CAT")
        .renameFieldVector("pk_schema_name", "PKTABLE_SCHEM")
        .renameFieldVector("pk_table_name", "PKTABLE_NAME")
        .renameFieldVector("pk_column_name", "PKCOLUMN_NAME")
        .renameFieldVector("fk_catalog_name", "FKTABLE_CAT")
        .renameFieldVector("fk_schema_name", "FKTABLE_SCHEM")
        .renameFieldVector("fk_table_name", "FKTABLE_NAME")
        .renameFieldVector("fk_column_name", "FKCOLUMN_NAME")
        .renameFieldVector("key_sequence", "KEY_SEQ")
        .renameFieldVector("fk_key_name", "FK_NAME")
        .renameFieldVector("pk_key_name", "PK_NAME")
        .renameFieldVector("update_rule", "UPDATE_RULE")
        .renameFieldVector("delete_rule", "DELETE_RULE")
        .addEmptyField("DEFERRABILITY", new ArrowType.Int(Byte.SIZE, false))
        .build();
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoSchemas = connection.getClientHandler().getSchemas(catalog, schemaPattern);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_SCHEMAS_SCHEMA, allocator)
            .renameFieldVector("schema_name", "TABLE_SCHEM")
            .renameFieldVector("catalog_name", "TABLE_CATALOG")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoSchemas, transformer);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoTableTypes = connection.getClientHandler().getTableTypes();

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_TABLE_TYPES_SCHEMA, allocator)
            .renameFieldVector("table_type", "TABLE_TYPE")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTableTypes, transformer);
  }

  @Override
  public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                             final String[] types)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoTables =
        connection.getClientHandler().getTables(catalog, schemaPattern, tableNamePattern, types, false);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_TABLES_SCHEMA_NO_SCHEMA, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .renameFieldVector("schema_name", "TABLE_SCHEM")
            .renameFieldVector("table_name", "TABLE_NAME")
            .renameFieldVector("table_type", "TABLE_TYPE")
            .addEmptyField("REMARKS", Types.MinorType.VARBINARY)
            .addEmptyField("TYPE_CAT", Types.MinorType.VARBINARY)
            .addEmptyField("TYPE_SCHEM", Types.MinorType.VARBINARY)
            .addEmptyField("TYPE_NAME", Types.MinorType.VARBINARY)
            .addEmptyField("SELF_REFERENCING_COL_NAME", Types.MinorType.VARBINARY)
            .addEmptyField("REF_GENERATION", Types.MinorType.VARBINARY)
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTables, transformer);
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoPrimaryKeys = connection.getClientHandler().getPrimaryKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_PRIMARY_KEYS_SCHEMA, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .renameFieldVector("schema_name", "TABLE_SCHEM")
            .renameFieldVector("table_name", "TABLE_NAME")
            .renameFieldVector("column_name", "COLUMN_NAME")
            .renameFieldVector("key_sequence", "KEY_SEQ")
            .renameFieldVector("key_name", "PK_NAME")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoPrimaryKeys, transformer);
  }

  @Override
  public ResultSet getColumns(final String catalog, final String schemaPattern, final String tableNamePattern,
                              final String columnNamePattern)
      throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoTables =
        connection.getClientHandler().getTables(catalog, schemaPattern, tableNamePattern, null, true);

    final BufferAllocator allocator = connection.getBufferAllocator();

    final Pattern columnNamePat = columnNamePattern != null ? Pattern.compile(sqlToRegexLike(columnNamePattern)) : null;

    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTables,
        (originalRoot, transformedRoot) -> {
          int columnCounter = 0;
          if (transformedRoot == null) {
            transformedRoot = VectorSchemaRoot.create(GET_COLUMNS_SCHEMA, allocator);
          }

          final int originalRootRowCount = originalRoot.getRowCount();

          final VarCharVector catalogNameVector = (VarCharVector) originalRoot.getVector("catalog_name");
          final VarCharVector tableNameVector = (VarCharVector) originalRoot.getVector("table_name");
          final VarCharVector schemaNameVector = (VarCharVector) originalRoot.getVector("schema_name");

          final VarBinaryVector schemaVector = (VarBinaryVector) originalRoot.getVector("table_schema");

          for (int i = 0; i < originalRootRowCount; i++) {
            final Schema currentSchema = MessageSerializer.deserializeSchema(
                Message.getRootAsMessage(
                    ByteBuffer.wrap(schemaVector.get(i))));

            final Text catalogName = catalogNameVector.getObject(i);
            final Text tableName = tableNameVector.getObject(i);
            final Text schemaName = schemaNameVector.getObject(i);

            final List<Field> tableColumns = currentSchema.getFields();

            columnCounter = setGetColumnsVectorSchemaRootFromFields(transformedRoot, columnCounter, tableColumns,
                catalogName, tableName, schemaName, columnNamePat);
          }

          transformedRoot.setRowCount(columnCounter);

          originalRoot.clear();
          return transformedRoot;
        });
  }

  private int setGetColumnsVectorSchemaRootFromFields(final VectorSchemaRoot currentRoot, int insertIndex,
                                                      final List<Field> tableColumns, final Text catalogName,
                                                      final Text tableName, final Text schemaName,
                                                      final Pattern columnNamePattern) {
    int ordinalIndex = 1;
    int tableColumnsSize = tableColumns.size();

    final VarCharVector tableCatVector = (VarCharVector) currentRoot.getVector("TABLE_CAT");
    final VarCharVector tableSchemVector = (VarCharVector) currentRoot.getVector("TABLE_SCHEM");
    final VarCharVector tableNameVector = (VarCharVector) currentRoot.getVector("TABLE_NAME");
    final VarCharVector columnNameVector = (VarCharVector) currentRoot.getVector("COLUMN_NAME");
    final IntVector dataTypeVector = (IntVector) currentRoot.getVector("DATA_TYPE");
    final VarCharVector typeNameVector = (VarCharVector) currentRoot.getVector("TYPE_NAME");
    final IntVector columnSizeVector = (IntVector) currentRoot.getVector("COLUMN_SIZE");
    final IntVector decimalDigitsVector = (IntVector) currentRoot.getVector("DECIMAL_DIGITS");
    final IntVector numPrecRadixVector = (IntVector) currentRoot.getVector("NUM_PREC_RADIX");
    final IntVector nullableVector = (IntVector) currentRoot.getVector("NULLABLE");
    final IntVector ordinalPositionVector = (IntVector) currentRoot.getVector("ORDINAL_POSITION");
    final VarCharVector isNullableVector = (VarCharVector) currentRoot.getVector("IS_NULLABLE");
    final VarCharVector isAutoincrementVector = (VarCharVector) currentRoot.getVector("IS_AUTOINCREMENT");
    final VarCharVector isGeneratedColumnVector = (VarCharVector) currentRoot.getVector("IS_GENERATEDCOLUMN");

    for (int i = 0; i < tableColumnsSize; i++, ordinalIndex++) {
      final String columnName = tableColumns.get(i).getName();

      if (columnNamePattern != null && !columnNamePattern.matcher(columnName).matches()) {
        continue;
      }

      final ArrowType fieldType = tableColumns.get(i).getType();

      if (catalogName != null) {
        tableCatVector.setSafe(insertIndex, catalogName);
      }

      if (schemaName != null) {
        tableSchemVector.setSafe(insertIndex, schemaName);
      }

      if (tableName != null) {
        tableNameVector.setSafe(insertIndex, tableName);
      }

      if (columnName != null) {
        columnNameVector.setSafe(insertIndex, columnName.getBytes(CHARSET));
      }

      dataTypeVector.setSafe(insertIndex, SqlTypes.getSqlTypeIdFromArrowType(fieldType));
      typeNameVector.setSafe(insertIndex, SqlTypes.getSqlTypeNameFromArrowType(fieldType).getBytes(CHARSET));

      // We're not setting COLUMN_SIZE for ROWID SQL Types, as there's no such Arrow type.
      // We're not setting COLUMN_SIZE nor DECIMAL_DIGITS for Float/Double as their precision and scale are variable.
      if (fieldType instanceof ArrowType.Decimal) {
        final ArrowType.Decimal thisDecimal = (ArrowType.Decimal) fieldType;
        columnSizeVector.setSafe(insertIndex, thisDecimal.getPrecision());
        decimalDigitsVector.setSafe(insertIndex, thisDecimal.getScale());
        numPrecRadixVector.setSafe(insertIndex, BASE10_RADIX);
      } else if (fieldType instanceof ArrowType.Int) {
        final ArrowType.Int thisInt = (ArrowType.Int) fieldType;
        switch (thisInt.getBitWidth()) {
          case Byte.SIZE:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_BYTE);
            break;
          case Short.SIZE:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_SHORT);
            break;
          case Integer.SIZE:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_INT);
            break;
          case Long.SIZE:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_LONG);
            break;
          default:
            columnSizeVector.setSafe(insertIndex,
                (int) Math.ceil((thisInt.getBitWidth() - 1) * Math.log(2) / Math.log(10)));
            break;
        }
        decimalDigitsVector.setSafe(insertIndex, NO_DECIMAL_DIGITS);
        numPrecRadixVector.setSafe(insertIndex, BASE10_RADIX);
      } else if (fieldType instanceof ArrowType.Utf8 || fieldType instanceof ArrowType.Binary) {
        columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_VARCHAR_AND_BINARY);
      } else if (fieldType instanceof ArrowType.Timestamp) {
        switch (((ArrowType.Timestamp) fieldType).getUnit()) {
          case SECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIMESTAMP_SECONDS);
            decimalDigitsVector.setSafe(insertIndex, NO_DECIMAL_DIGITS);
            break;
          case MILLISECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIMESTAMP_MILLISECONDS);
            decimalDigitsVector.setSafe(insertIndex, DECIMAL_DIGITS_TIME_MILLISECONDS);
            break;
          case MICROSECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIMESTAMP_MICROSECONDS);
            decimalDigitsVector.setSafe(insertIndex, DECIMAL_DIGITS_TIME_MICROSECONDS);
            break;
          case NANOSECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIMESTAMP_NANOSECONDS);
            decimalDigitsVector.setSafe(insertIndex, DECIMAL_DIGITS_TIME_NANOSECONDS);
            break;
          default:
            break;
        }
      } else if (fieldType instanceof ArrowType.Time) {
        switch (((ArrowType.Time) fieldType).getUnit()) {
          case SECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIME);
            decimalDigitsVector.setSafe(insertIndex, NO_DECIMAL_DIGITS);
            break;
          case MILLISECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIME_MILLISECONDS);
            decimalDigitsVector.setSafe(insertIndex, DECIMAL_DIGITS_TIME_MILLISECONDS);
            break;
          case MICROSECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIME_MICROSECONDS);
            decimalDigitsVector.setSafe(insertIndex, DECIMAL_DIGITS_TIME_MICROSECONDS);
            break;
          case NANOSECOND:
            columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_TIME_NANOSECONDS);
            decimalDigitsVector.setSafe(insertIndex, DECIMAL_DIGITS_TIME_NANOSECONDS);
            break;
          default:
            break;
        }
      } else if (fieldType instanceof ArrowType.Date) {
        columnSizeVector.setSafe(insertIndex, COLUMN_SIZE_DATE);
        decimalDigitsVector.setSafe(insertIndex, NO_DECIMAL_DIGITS);
      } else if (fieldType instanceof ArrowType.FloatingPoint) {
        numPrecRadixVector.setSafe(insertIndex, BASE10_RADIX);
      }

      nullableVector.setSafe(insertIndex, tableColumns.get(i).isNullable() ? 1 : 0);

      isNullableVector.setSafe(insertIndex,
          tableColumns.get(i).isNullable() ? "YES".getBytes(CHARSET) : "NO".getBytes(CHARSET));

      // Fields also don't hold information about IS_AUTOINCREMENT and IS_GENERATEDCOLUMN,
      // so we're setting an empty string (as bytes), which means it couldn't be determined.
      isAutoincrementVector.setSafe(insertIndex, EMPTY_BYTE_ARRAY);
      isGeneratedColumnVector.setSafe(insertIndex, EMPTY_BYTE_ARRAY);

      ordinalPositionVector.setSafe(insertIndex, ordinalIndex);

      insertIndex++;
    }
    return insertIndex;
  }

  private String sqlToRegexLike(final String sqlPattern) {
    final char escapeChar = (char) 0;
    final int len = sqlPattern.length();
    final StringBuilder javaPattern = new StringBuilder(len + len);

    for (int i = 0; i < len; i++) {
      char currentChar = sqlPattern.charAt(i);

      if (JAVA_REGEX_SPECIALS.indexOf(currentChar) >= 0) {
        javaPattern.append('\\');
      }

      switch (currentChar) {
        case escapeChar:
          char nextChar = sqlPattern.charAt(i + 1);
          if ((nextChar == '_') || (nextChar == '%') || (nextChar == escapeChar)) {
            javaPattern.append(nextChar);
            i++;
          }
          break;
        case '_':
          javaPattern.append('.');
          break;
        case '%':
          javaPattern.append(".");
          javaPattern.append('*');
          break;
        default:
          javaPattern.append(currentChar);
          break;
      }
    }
    return javaPattern.toString();
  }
}

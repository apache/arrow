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

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

/**
 * Arrow Flight JDBC's implementation of {@link DatabaseMetaData}.
 */
public class ArrowDatabaseMetadata extends AvaticaDatabaseMetaData {

  protected ArrowDatabaseMetadata(final ArrowFlightConnection connection) {
    super(connection);
  }

  @Override
  public ArrowFlightConnection getConnection() throws SQLException {
    return (ArrowFlightConnection) super.getConnection();
  }

  @Override
  public ResultSet getCatalogs() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoCatalogs = connection.getClientHandler().getCatalogs();

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_CATALOGS, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoCatalogs, transformer);
  }

  @Override
  public ResultSet getImportedKeys(final String catalog, final String schema, final String table) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoImportedKeys = connection.getClientHandler().getImportedKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_IMPORTED_AND_EXPORTED_KEYS, allocator)
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
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoImportedKeys, transformer);
  }

  @Override
  public ResultSet getExportedKeys(final String catalog, final String schema, final String table) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoExportedKeys = connection.getClientHandler().getExportedKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_IMPORTED_AND_EXPORTED_KEYS, allocator)
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
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoExportedKeys, transformer);
  }

  @Override
  public ResultSet getSchemas(final String catalog, final String schemaPattern) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoSchemas = connection.getClientHandler().getSchemas(catalog, schemaPattern);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_SCHEMAS, allocator)
            .renameFieldVector("catalog_name", "TABLE_CATALOG")
            .renameFieldVector("schema_name", "TABLE_SCHEM")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoSchemas, transformer);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoTableTypes = connection.getClientHandler().getTableTypes();

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_TABLE_TYPES, allocator)
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
        new VectorSchemaRootTransformer.Builder(Schemas.GET_TABLES, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .renameFieldVector("schema_name", "TABLE_SCHEM")
            .renameFieldVector("table_name", "TABLE_NAME")
            .renameFieldVector("table_type", "TABLE_TYPE")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoTables, transformer);
  }

  @Override
  public ResultSet getPrimaryKeys(final String catalog, final String schema, final String table) throws SQLException {
    final ArrowFlightConnection connection = getConnection();
    final FlightInfo flightInfoPrimaryKeys = connection.getClientHandler().getPrimaryKeys(catalog, schema, table);

    final BufferAllocator allocator = connection.getBufferAllocator();
    final VectorSchemaRootTransformer transformer =
        new VectorSchemaRootTransformer.Builder(Schemas.GET_PRIMARY_KEYS, allocator)
            .renameFieldVector("catalog_name", "TABLE_CAT")
            .renameFieldVector("schema_name", "TABLE_SCHEM")
            .renameFieldVector("table_name", "TABLE_NAME")
            .renameFieldVector("column_name", "COLUMN_NAME")
            .renameFieldVector("key_sequence", "KEY_SEQ")
            .renameFieldVector("key_name", "PK_NAME")
            .build();
    return ArrowFlightJdbcFlightStreamResultSet.fromFlightInfo(connection, flightInfoPrimaryKeys, transformer);
  }

  /**
   * Base Schemas for {@link VectorSchemaRootTransformer} use.
   */
  private static class Schemas {
    public static final Schema GET_TABLES = new Schema(Arrays.asList(
        Field.nullable("TABLE_CAT", Types.MinorType.VARCHAR.getType()),
        Field.nullable("TABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("TABLE_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("TABLE_TYPE", Types.MinorType.VARCHAR.getType()),
        // TODO Add these fields to FlightSQL, as it's currently not possible to fetch them.
        Field.nullable("REMARKS", Types.MinorType.VARBINARY.getType()),
        Field.nullable("TYPE_CAT", Types.MinorType.VARBINARY.getType()),
        Field.nullable("TYPE_SCHEM", Types.MinorType.VARBINARY.getType()),
        Field.nullable("TYPE_NAME", Types.MinorType.VARBINARY.getType()),
        Field.nullable("SELF_REFERENCING_COL_NAME", Types.MinorType.VARBINARY.getType()),
        Field.nullable("REF_GENERATION", Types.MinorType.VARBINARY.getType())
    ));

    private static final Schema GET_CATALOGS = new Schema(
        Collections.singletonList(Field.notNullable("TABLE_CAT", Types.MinorType.VARCHAR.getType())));

    private static final Schema GET_TABLE_TYPES =
        new Schema(Collections.singletonList(Field.notNullable("TABLE_TYPE", Types.MinorType.VARCHAR.getType())));

    private static final Schema GET_SCHEMAS = new Schema(
        Arrays.asList(Field.notNullable("TABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
            Field.nullable("TABLE_CATALOG", Types.MinorType.VARCHAR.getType())));

    private static final Schema GET_IMPORTED_AND_EXPORTED_KEYS = new Schema(Arrays.asList(
        Field.nullable("PKTABLE_CAT", Types.MinorType.VARCHAR.getType()),
        Field.nullable("PKTABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("PKTABLE_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("PKCOLUMN_NAME", Types.MinorType.VARCHAR.getType()),
        Field.nullable("FKTABLE_CAT", Types.MinorType.VARCHAR.getType()),
        Field.nullable("FKTABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("FKTABLE_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("FKCOLUMN_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("KEY_SEQ", Types.MinorType.INT.getType()),
        Field.nullable("FK_NAME", Types.MinorType.VARCHAR.getType()),
        Field.nullable("PK_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("UPDATE_RULE", new ArrowType.Int(Byte.SIZE, false)),
        Field.notNullable("DELETE_RULE", new ArrowType.Int(Byte.SIZE, false)),
        // TODO Add this field to FlightSQL, as it's currently not possible to fetch them.
        Field.notNullable("DEFERRABILITY", new ArrowType.Int(Byte.SIZE, false))));

    private static final Schema GET_PRIMARY_KEYS = new Schema(Arrays.asList(
        Field.nullable("TABLE_CAT", Types.MinorType.VARCHAR.getType()),
        Field.nullable("TABLE_SCHEM", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("TABLE_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("COLUMN_NAME", Types.MinorType.VARCHAR.getType()),
        Field.notNullable("KEY_SEQ", Types.MinorType.INT.getType()),
        Field.nullable("PK_NAME", Types.MinorType.VARCHAR.getType())));
  }
}

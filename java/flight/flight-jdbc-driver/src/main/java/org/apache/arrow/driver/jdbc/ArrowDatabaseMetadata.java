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

import org.apache.arrow.driver.jdbc.utils.VectorSchemaRootTransformer;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.sql.FlightSqlProducer.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

/**
 * Arrow Flight JDBC's implementation of {@link DatabaseMetaData}.
 */
public class ArrowDatabaseMetadata extends AvaticaDatabaseMetaData {

  protected ArrowDatabaseMetadata(final AvaticaConnection connection) {
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
}

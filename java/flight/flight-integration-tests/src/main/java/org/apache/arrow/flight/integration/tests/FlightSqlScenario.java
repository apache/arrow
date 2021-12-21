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

package org.apache.arrow.flight.integration.tests;

import java.util.Arrays;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Integration test scenario for validating Flight SQL specs across multiple implementations.
 * This should ensure that RPC objects are being built and parsed correctly for multiple languages
 * and that the Arrow schemas are returned as expected.
 */
public class FlightSqlScenario implements Scenario {

  public static final long UPDATE_STATEMENT_EXPECTED_ROWS = 10000L;
  public static final long UPDATE_PREPARED_STATEMENT_EXPECTED_ROWS = 20000L;

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new FlightSqlScenarioProducer(allocator);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) throws Exception {

  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client)
      throws Exception {
    final FlightSqlClient sqlClient = new FlightSqlClient(client);

    validateMetadataRetrieval(sqlClient);

    validateStatementExecution(sqlClient);

    validatePreparedStatementExecution(sqlClient, allocator);
  }

  private void validateMetadataRetrieval(FlightSqlClient sqlClient) throws Exception {
    final CallOption[] options = new CallOption[0];

    validate(FlightSqlProducer.Schemas.GET_CATALOGS_SCHEMA, sqlClient.getCatalogs(options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_SCHEMAS_SCHEMA,
        sqlClient.getSchemas("catalog", "db_schema_filter_pattern", options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_TABLES_SCHEMA,
        sqlClient.getTables("catalog", "db_schema_filter_pattern", "table_filter_pattern",
            Arrays.asList("table", "view"), true, options), sqlClient);
    validate(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA, sqlClient.getTableTypes(options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_PRIMARY_KEYS_SCHEMA,
        sqlClient.getPrimaryKeys(TableRef.of("catalog", "db_schema", "table"), options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_EXPORTED_KEYS_SCHEMA,
        sqlClient.getExportedKeys(TableRef.of("catalog", "db_schema", "table"), options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_IMPORTED_KEYS_SCHEMA,
        sqlClient.getImportedKeys(TableRef.of("catalog", "db_schema", "table"), options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_CROSS_REFERENCE_SCHEMA,
        sqlClient.getCrossReference(TableRef.of("pk_catalog", "pk_db_schema", "pk_table"),
            TableRef.of("fk_catalog", "fk_db_schema", "fk_table"), options),
        sqlClient);
    validate(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA,
        sqlClient.getSqlInfo(new FlightSql.SqlInfo[] {FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
            FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY}, options), sqlClient);
  }

  private void validateStatementExecution(FlightSqlClient sqlClient) throws Exception {
    final CallOption[] options = new CallOption[0];

    validate(FlightSqlScenarioProducer.getQuerySchema(),
        sqlClient.execute("SELECT STATEMENT", options), sqlClient);

    IntegrationAssertions.assertEquals(sqlClient.executeUpdate("UPDATE STATEMENT", options),
        UPDATE_STATEMENT_EXPECTED_ROWS);
  }

  private void validatePreparedStatementExecution(FlightSqlClient sqlClient,
                                                  BufferAllocator allocator) throws Exception {
    final CallOption[] options = new CallOption[0];
    try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(
        "SELECT PREPARED STATEMENT");
         VectorSchemaRoot parameters = VectorSchemaRoot.create(
             FlightSqlScenarioProducer.getQuerySchema(), allocator)) {
      parameters.setRowCount(1);
      preparedStatement.setParameters(parameters);

      validate(FlightSqlScenarioProducer.getQuerySchema(), preparedStatement.execute(options),
          sqlClient);
    }

    try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(
        "UPDATE PREPARED STATEMENT")) {
      IntegrationAssertions.assertEquals(preparedStatement.executeUpdate(options),
          UPDATE_PREPARED_STATEMENT_EXPECTED_ROWS);
    }
  }

  private void validate(Schema expectedSchema, FlightInfo flightInfo,
                        FlightSqlClient sqlClient) throws Exception {
    Ticket ticket = flightInfo.getEndpoints().get(0).getTicket();
    try (FlightStream stream = sqlClient.getStream(ticket)) {
      Schema actualSchema = stream.getSchema();
      IntegrationAssertions.assertEquals(expectedSchema, actualSchema);
    }
  }
}

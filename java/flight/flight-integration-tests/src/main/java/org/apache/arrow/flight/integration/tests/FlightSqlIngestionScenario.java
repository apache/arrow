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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.ExecuteIngestOptions;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.CommandStatementIngest.TableDefinitionOptions;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Integration test scenario for validating Flight SQL specs across multiple implementations. This
 * should ensure that RPC objects are being built and parsed correctly for multiple languages and
 * that the Arrow schemas are returned as expected.
 */
public class FlightSqlIngestionScenario extends FlightSqlScenario {

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    FlightSqlScenarioProducer producer =
        (FlightSqlScenarioProducer) super.producer(allocator, location);
    producer
        .getSqlInfoBuilder()
        .withFlightSqlServerBulkIngestionTransaction(true)
        .withFlightSqlServerBulkIngestion(true);
    return producer;
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client)
      throws Exception {
    try (final FlightSqlClient sqlClient = new FlightSqlClient(client)) {
      validateMetadataRetrieval(sqlClient);
      validateIngestion(allocator, sqlClient);
    }
  }

  private void validateMetadataRetrieval(FlightSqlClient sqlClient) throws Exception {
    validate(
        FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA,
        sqlClient.getSqlInfo(
            FlightSql.SqlInfo.FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED,
            FlightSql.SqlInfo.FLIGHT_SQL_SERVER_BULK_INGESTION),
        sqlClient,
        s -> {
          Map<Integer, Object> infoValues = readSqlInfoStream(s);
          IntegrationAssertions.assertEquals(
              Boolean.TRUE,
              infoValues.get(
                  FlightSql.SqlInfo.FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED_VALUE));
          IntegrationAssertions.assertEquals(
              Boolean.TRUE,
              infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_BULK_INGESTION_VALUE));
        });
  }

  private VectorSchemaRoot getIngestVectorRoot(BufferAllocator allocator) {
    Schema schema = FlightSqlScenarioProducer.getIngestSchema();
    VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
    root.setRowCount(3);
    return root;
  }

  private void validateIngestion(BufferAllocator allocator, FlightSqlClient sqlClient) {
    try (VectorSchemaRoot data = getIngestVectorRoot(allocator)) {
      TableDefinitionOptions tableDefinitionOptions =
          TableDefinitionOptions.newBuilder()
              .setIfExists(TableDefinitionOptions.TableExistsOption.TABLE_EXISTS_OPTION_REPLACE)
              .setIfNotExist(
                  TableDefinitionOptions.TableNotExistOption.TABLE_NOT_EXIST_OPTION_CREATE)
              .build();
      Map<String, String> options = new HashMap<>(ImmutableMap.of("key1", "val1", "key2", "val2"));
      ExecuteIngestOptions executeIngestOptions =
          new ExecuteIngestOptions(
              "test_table", tableDefinitionOptions, true, "test_catalog", "test_schema", options);
      FlightSqlClient.Transaction transaction =
          new FlightSqlClient.Transaction(BULK_INGEST_TRANSACTION_ID);
      long updatedRows = sqlClient.executeIngest(data, executeIngestOptions, transaction);

      IntegrationAssertions.assertEquals(3L, updatedRows);
    }
  }
}

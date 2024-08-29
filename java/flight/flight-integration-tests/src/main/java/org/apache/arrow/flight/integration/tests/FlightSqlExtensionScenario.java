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

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.sql.CancelResult;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Integration test scenario for validating Flight SQL specs across multiple implementations.
 * This should ensure that RPC objects are being built and parsed correctly for multiple languages
 * and that the Arrow schemas are returned as expected.
 */
public class FlightSqlExtensionScenario extends FlightSqlScenario {
  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client)
      throws Exception {
    try (final FlightSqlClient sqlClient = new FlightSqlClient(client)) {
      validateMetadataRetrieval(sqlClient);
      validateStatementExecution(sqlClient);
      validatePreparedStatementExecution(allocator, sqlClient);
      validateTransactions(allocator, sqlClient);
    }
  }

  private void validateMetadataRetrieval(FlightSqlClient sqlClient) throws Exception {
    FlightInfo info = sqlClient.getSqlInfo();
    Ticket ticket = info.getEndpoints().get(0).getTicket();

    Map<Integer, Object> infoValues = new HashMap<>();
    try (FlightStream stream = sqlClient.getStream(ticket)) {
      Schema actualSchema = stream.getSchema();
      IntegrationAssertions.assertEquals(FlightSqlProducer.Schemas.GET_SQL_INFO_SCHEMA, actualSchema);

      while (stream.next()) {
        UInt4Vector infoName = (UInt4Vector) stream.getRoot().getVector(0);
        DenseUnionVector value = (DenseUnionVector) stream.getRoot().getVector(1);

        for (int i = 0; i < stream.getRoot().getRowCount(); i++) {
          final int code = infoName.get(i);
          if (infoValues.containsKey(code)) {
            throw new AssertionError("Duplicate SqlInfo value: " + code);
          }
          Object object;
          byte typeId = value.getTypeId(i);
          switch (typeId) {
            case 0: // string
              object = Preconditions.checkNotNull(value.getVarCharVector(typeId)
                  .getObject(value.getOffset(i)))
                  .toString();
              break;
            case 1: // bool
              object = value.getBitVector(typeId).getObject(value.getOffset(i));
              break;
            case 2: // int64
              object = value.getBigIntVector(typeId).getObject(value.getOffset(i));
              break;
            case 3: // int32
              object = value.getIntVector(typeId).getObject(value.getOffset(i));
              break;
            default:
              throw new AssertionError("Decoding SqlInfo of type code " + typeId);
          }
          infoValues.put(code, object);
        }
      }
    }

    IntegrationAssertions.assertEquals(Boolean.FALSE,
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_SQL_VALUE));
    IntegrationAssertions.assertEquals(Boolean.TRUE,
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_VALUE));
    IntegrationAssertions.assertEquals("min_version",
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION_VALUE));
    IntegrationAssertions.assertEquals("max_version",
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION_VALUE));
    IntegrationAssertions.assertEquals(FlightSql.SqlSupportedTransaction.SQL_SUPPORTED_TRANSACTION_SAVEPOINT_VALUE,
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_VALUE));
    IntegrationAssertions.assertEquals(Boolean.TRUE,
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_CANCEL_VALUE));
    IntegrationAssertions.assertEquals(42,
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT_VALUE));
    IntegrationAssertions.assertEquals(7,
        infoValues.get(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT_VALUE));
  }

  private void validateStatementExecution(FlightSqlClient sqlClient) throws Exception {
    FlightInfo info = sqlClient.executeSubstrait(SUBSTRAIT_PLAN);
    validate(FlightSqlScenarioProducer.getQuerySchema(), info, sqlClient);

    SchemaResult result = sqlClient.getExecuteSubstraitSchema(SUBSTRAIT_PLAN);
    validateSchema(FlightSqlScenarioProducer.getQuerySchema(), result);

    IntegrationAssertions.assertEquals(CancelResult.CANCELLED, sqlClient.cancelQuery(info));

    IntegrationAssertions.assertEquals(sqlClient.executeSubstraitUpdate(SUBSTRAIT_PLAN),
        UPDATE_STATEMENT_EXPECTED_ROWS);
  }

  private void validatePreparedStatementExecution(BufferAllocator allocator,
                                                  FlightSqlClient sqlClient) throws Exception {
    try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(SUBSTRAIT_PLAN);
         VectorSchemaRoot parameters = VectorSchemaRoot.create(
             FlightSqlScenarioProducer.getQuerySchema(), allocator)) {
      parameters.setRowCount(1);
      preparedStatement.setParameters(parameters);
      validate(FlightSqlScenarioProducer.getQuerySchema(), preparedStatement.execute(), sqlClient);
      validateSchema(FlightSqlScenarioProducer.getQuerySchema(), preparedStatement.fetchSchema());
    }

    try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(SUBSTRAIT_PLAN)) {
      IntegrationAssertions.assertEquals(preparedStatement.executeUpdate(),
          UPDATE_PREPARED_STATEMENT_EXPECTED_ROWS);
    }
  }

  private void validateTransactions(BufferAllocator allocator, FlightSqlClient sqlClient) throws Exception {
    final FlightSqlClient.Transaction transaction = sqlClient.beginTransaction();
    IntegrationAssertions.assertEquals(TRANSACTION_ID, transaction.getTransactionId());

    final FlightSqlClient.Savepoint savepoint = sqlClient.beginSavepoint(transaction, SAVEPOINT_NAME);
    IntegrationAssertions.assertEquals(SAVEPOINT_ID, savepoint.getSavepointId());

    FlightInfo info = sqlClient.execute("SELECT STATEMENT", transaction);
    validate(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), info, sqlClient);

    info = sqlClient.executeSubstrait(SUBSTRAIT_PLAN, transaction);
    validate(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), info, sqlClient);

    SchemaResult schema = sqlClient.getExecuteSchema("SELECT STATEMENT", transaction);
    validateSchema(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), schema);

    schema = sqlClient.getExecuteSubstraitSchema(SUBSTRAIT_PLAN, transaction);
    validateSchema(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), schema);

    IntegrationAssertions.assertEquals(sqlClient.executeUpdate("UPDATE STATEMENT", transaction),
        UPDATE_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);
    IntegrationAssertions.assertEquals(sqlClient.executeSubstraitUpdate(SUBSTRAIT_PLAN, transaction),
        UPDATE_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);

    try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(
        "SELECT PREPARED STATEMENT", transaction);
         VectorSchemaRoot parameters = VectorSchemaRoot.create(
             FlightSqlScenarioProducer.getQuerySchema(), allocator)) {
      parameters.setRowCount(1);
      preparedStatement.setParameters(parameters);
      validate(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), preparedStatement.execute(), sqlClient);
      schema = preparedStatement.fetchSchema();
      validateSchema(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), schema);
    }

    try (FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(SUBSTRAIT_PLAN, transaction);
         VectorSchemaRoot parameters = VectorSchemaRoot.create(
             FlightSqlScenarioProducer.getQuerySchema(), allocator)) {
      parameters.setRowCount(1);
      preparedStatement.setParameters(parameters);
      validate(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), preparedStatement.execute(), sqlClient);
      schema = preparedStatement.fetchSchema();
      validateSchema(FlightSqlScenarioProducer.getQueryWithTransactionSchema(), schema);
    }

    try (FlightSqlClient.PreparedStatement preparedStatement =
             sqlClient.prepare("UPDATE PREPARED STATEMENT", transaction)) {
      IntegrationAssertions.assertEquals(preparedStatement.executeUpdate(),
          UPDATE_PREPARED_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);
    }

    try (FlightSqlClient.PreparedStatement preparedStatement =
             sqlClient.prepare(SUBSTRAIT_PLAN, transaction)) {
      IntegrationAssertions.assertEquals(preparedStatement.executeUpdate(),
          UPDATE_PREPARED_STATEMENT_WITH_TRANSACTION_EXPECTED_ROWS);
    }

    sqlClient.rollback(savepoint);

    final FlightSqlClient.Savepoint savepoint2 = sqlClient.beginSavepoint(transaction, SAVEPOINT_NAME);
    IntegrationAssertions.assertEquals(SAVEPOINT_ID, savepoint2.getSavepointId());
    sqlClient.release(savepoint);

    sqlClient.commit(transaction);

    final FlightSqlClient.Transaction transaction2 = sqlClient.beginTransaction();
    IntegrationAssertions.assertEquals(TRANSACTION_ID, transaction2.getTransactionId());
    sqlClient.rollback(transaction);
  }
}

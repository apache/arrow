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
package org.apache.arrow.flight.sql.test;

import static org.apache.arrow.flight.sql.util.FlightStreamUtils.getResults;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.hamcrest.CoreMatchers.*;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.example.FlightSqlStatelessExample;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test direct usage of Flight SQL workflows. */
public class TestFlightSqlStateless extends TestFlightSql {

  @BeforeAll
  public static void setUp() throws Exception {
    setUpClientServer();
    setUpExpectedResultsMap();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    close(sqlClient, server, allocator);
    FlightSqlStatelessExample.removeDerbyDatabaseIfExists(FlightSqlStatelessExample.DB_NAME);
  }

  private static void setUpClientServer() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    server =
        FlightServer.builder(
                allocator,
                serverLocation,
                new FlightSqlStatelessExample(serverLocation, FlightSqlStatelessExample.DB_NAME))
            .build()
            .start();

    final Location clientLocation = Location.forGrpcInsecure(LOCALHOST, server.getPort());
    sqlClient = new FlightSqlClient(FlightClient.builder(allocator, clientLocation).build());
  }

  @Override
  @Test
  public void testSimplePreparedStatementResultsWithParameterBinding() throws Exception {
    try (PreparedStatement prepare = sqlClient.prepare("SELECT * FROM intTable WHERE id = ?")) {
      final Schema parameterSchema = prepare.getParameterSchema();
      try (final VectorSchemaRoot insertRoot =
          VectorSchemaRoot.create(parameterSchema, allocator)) {
        insertRoot.allocateNew();

        final IntVector valueVector = (IntVector) insertRoot.getVector(0);
        valueVector.setSafe(0, 1);
        insertRoot.setRowCount(1);

        prepare.setParameters(insertRoot);
        final FlightInfo flightInfo = prepare.execute();

        for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
          try (FlightStream stream = sqlClient.getStream(endpoint.getTicket())) {
            Assertions.assertAll(
                () -> MatcherAssert.assertThat(stream.getSchema(), is(SCHEMA_INT_TABLE)),
                () ->
                    MatcherAssert.assertThat(
                        getResults(stream), is(EXPECTED_RESULTS_FOR_PARAMETER_BINDING)));
          }
        }
      }
    }
  }
}

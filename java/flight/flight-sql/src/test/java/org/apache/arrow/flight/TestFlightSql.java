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

package org.apache.arrow.flight;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.isNull;
import static org.apache.arrow.util.AutoCloseables.close;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.FlightSqlClient.PreparedStatement;
import org.apache.arrow.flight.sql.FlightSqlColumnMetadata;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlSupportedCaseSensitivity;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import com.google.common.collect.ImmutableList;

/**
 * Test direct usage of Flight SQL workflows.
 */
public class TestFlightSql {

  protected static final Schema SCHEMA_INT_TABLE = new Schema(asList(
      new Field("ID", new FieldType(false, MinorType.INT.getType(), null), null),
      Field.nullable("KEYNAME", MinorType.VARCHAR.getType()),
      Field.nullable("VALUE", MinorType.INT.getType()),
      Field.nullable("FOREIGNID", MinorType.INT.getType())));
  private static final List<List<String>> EXPECTED_RESULTS_FOR_STAR_SELECT_QUERY = ImmutableList.of(
      asList("1", "one", "1", "1"), asList("2", "zero", "0", "1"), asList("3", "negative one", "-1", "1"));
  private static final List<List<String>> EXPECTED_RESULTS_FOR_PARAMETER_BINDING = ImmutableList.of(
      asList("1", "one", "1", "1"));
  private static final Map<String, String> GET_SQL_INFO_EXPECTED_RESULTS_MAP = new LinkedHashMap<>();
  private static final String LOCALHOST = "localhost";
  private static BufferAllocator allocator;

  @BeforeAll
  public static void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);

    final Location serverLocation = Location.forGrpcInsecure(LOCALHOST, 0);

    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME_VALUE), "Apache Derby");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION_VALUE), "10.14.2.0 - (1828579)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION_VALUE), "10.14.2.0 - (1828579)");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY_VALUE), "false");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_DDL_CATALOG_VALUE), "false");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_DDL_SCHEMA_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_DDL_TABLE_VALUE), "true");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(
            Integer.toString(FlightSql.SqlInfo.SQL_IDENTIFIER_CASE_VALUE),
            Integer.toString(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_UPPERCASE_VALUE));
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(Integer.toString(FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR_VALUE), "\"");
    GET_SQL_INFO_EXPECTED_RESULTS_MAP
        .put(
            Integer.toString(FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE_VALUE),
            Integer.toString(SqlSupportedCaseSensitivity.SQL_CASE_SENSITIVITY_CASE_INSENSITIVE_VALUE));
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(final List<? extends List<String>> results) {
    return getNonConformingResultsForGetSqlInfo(results,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_NAME,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_VERSION,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_ARROW_VERSION,
        FlightSql.SqlInfo.FLIGHT_SQL_SERVER_READ_ONLY,
        FlightSql.SqlInfo.SQL_DDL_CATALOG,
        FlightSql.SqlInfo.SQL_DDL_SCHEMA,
        FlightSql.SqlInfo.SQL_DDL_TABLE,
        FlightSql.SqlInfo.SQL_IDENTIFIER_CASE,
        FlightSql.SqlInfo.SQL_IDENTIFIER_QUOTE_CHAR,
        FlightSql.SqlInfo.SQL_QUOTED_IDENTIFIER_CASE);
  }

  private static List<List<String>> getNonConformingResultsForGetSqlInfo(
      final List<? extends List<String>> results,
      final FlightSql.SqlInfo... args) {
    final List<List<String>> nonConformingResults = new ArrayList<>();
    if (results.size() == args.length) {
      for (int index = 0; index < results.size(); index++) {
        final List<String> result = results.get(index);
        final String providedName = result.get(0);
        final String expectedName = Integer.toString(args[index].getNumber());
        if (!(GET_SQL_INFO_EXPECTED_RESULTS_MAP.get(providedName).equals(result.get(1)) &&
            providedName.equals(expectedName))) {
          nonConformingResults.add(result);
          break;
        }
      }
    }
    return nonConformingResults;
  }

  @Test
  public void testStmt() throws Exception {
    String username = "admin";
    String password = "password";
    final Location clientLocation = Location.forGrpcInsecure("127.0.0.1", 50060);
    FlightClient.Builder flightBuilder = FlightClient.builder(allocator, clientLocation);
    ClientIncomingAuthHeaderMiddleware.Factory authFactory =
            new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
    flightBuilder.intercept(authFactory);
    FlightClientMiddleware.Factory cookieFactory = new ClientCookieMiddleware.Factory();
    flightBuilder.intercept(cookieFactory);
    FlightClient flightClient = flightBuilder.build();
    CredentialCallOption basic = new CredentialCallOption(new BasicAuthCredentialWriter(username, password));
    flightClient.handshake(basic);

    FlightSqlClient sqlClient = new FlightSqlClient(flightClient);
    FlightSqlClient.PreparedStatement ps = sqlClient.prepare("select 'Hello, FlightSQL!' as salutation", authFactory.getCredentialCallOption());
    Schema resultSetSchema = ps.getResultSetSchema();
    Schema parameterSchema = ps.getParameterSchema();
    for(int i = 0; i < 10; i++) {
      int rows = 0;
      FlightInfo fi = ps.execute(authFactory.getCredentialCallOption()); // 3ms
      Schema schema = fi.getSchema(); // 0ms
      for (FlightEndpoint ep : fi.getEndpoints()) {
        Ticket ticket = ep.getTicket(); // 0ms
        FlightStream stream = sqlClient.getStream(ticket, authFactory.getCredentialCallOption()); // 1ms

        long start = System.currentTimeMillis();
        final VectorSchemaRoot root = stream.getRoot(); // 40ms
        long end = System.currentTimeMillis();
        System.out.format("Got %d row in %dms\n", rows, end - start);

        try {
          VarCharVector a = (VarCharVector) root.getVector(0);
//          BigIntVector a = (BigIntVector) root.getVector(0);
          while (stream.next()) {
            rows = root.getRowCount();
            for (int row_idx = 0; row_idx < rows; row_idx++) {
//              long res = a.get(row_idx);
//              System.out.format("s_quantity=%d\n", res);
              byte[] res = a.get(row_idx);
              System.out.format("s_quantity=%d\n", res.length);
            }
          }
        } finally {
          root.clear();
        }
      }
    }
    sqlClient.close();
  }

}

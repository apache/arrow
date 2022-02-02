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
import static java.util.stream.IntStream.range;
import static org.hamcrest.CoreMatchers.is;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.function.Consumer;

import org.apache.arrow.driver.jdbc.adhoc.MockFlightSqlProducer;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.sql.FlightSqlProducer;
import org.apache.arrow.flight.sql.impl.FlightSql;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import com.google.protobuf.Message;

public class FlightCookieTest {

  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();
  private static final String REGULAR_QUERY_SAMPLE = "SELECT * FROM NOT_UPDATE_QUERY";
  private static final Schema REGULAR_QUERY_SCHEMA =
      new Schema(
          Collections.singletonList(Field.nullable("placeholder", Types.MinorType.VARCHAR.getType())));
  private static final int ROW_COUNT = 10;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createStandardTestRule(FLIGHT_SQL_PRODUCER);

  @BeforeClass
  public static void setup() {
    FLIGHT_SQL_PRODUCER.addSelectQuery(
        REGULAR_QUERY_SAMPLE,
        REGULAR_QUERY_SCHEMA,
        Collections.singletonList(listener -> {
          try (final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
               final VectorSchemaRoot root = VectorSchemaRoot.create(REGULAR_QUERY_SCHEMA,
                   allocator)) {
            listener.start(root);
            listener.putNext();
          } catch (final Throwable throwable) {
            listener.error(throwable);
          } finally {
            listener.completed();
          }
        }));
    final Message commandGetTableTypes = FlightSql.CommandGetTableTypes.getDefaultInstance();
    final Consumer<FlightProducer.ServerStreamListener> commandGetTableTypesResultProducer = listener -> {
      try (final BufferAllocator allocator = new RootAllocator();
           final VectorSchemaRoot root = VectorSchemaRoot.create(FlightSqlProducer.Schemas.GET_TABLE_TYPES_SCHEMA,
               allocator)) {
        final VarCharVector tableType = (VarCharVector) root.getVector("table_type");
        range(0, ROW_COUNT).forEach(
            i -> tableType.setSafe(i, new Text(format("table_type #%d", i))));
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
  }

  @Test
  public void testCookies() throws SQLException {
    try (Connection connection = FLIGHT_SERVER_TEST_RULE.getConnection();
         Statement statement = connection.createStatement()) {

      // Since the first cookie was just updated by the client it was not send yet.
      // That's why we're checking for null.
      Assert.assertNull(FLIGHT_SERVER_TEST_RULE.getFactory().getCookie());

      // Run another action for check if the cookies was sent by the client.
      statement.execute(REGULAR_QUERY_SAMPLE);
      collector.checkThat(statement.execute(REGULAR_QUERY_SAMPLE),
          is(true)); // Meaning there was a select query.
      Assert.assertEquals("k=v", FLIGHT_SERVER_TEST_RULE.getFactory().getCookie());

      //Run another command to check if cookies keep being sent by subsequent requests
      ResultSet resultSet = connection.getMetaData().getTableTypes();
      Assert.assertEquals("k=v", FLIGHT_SERVER_TEST_RULE.getFactory().getCookie());

      // Closes resources
      resultSet.close();
    }
  }
}


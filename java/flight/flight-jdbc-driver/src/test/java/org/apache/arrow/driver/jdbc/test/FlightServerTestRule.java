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

package org.apache.arrow.driver.jdbc.test;

import static org.apache.arrow.driver.jdbc.utils.BaseProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.BaseProperty.USERNAME;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDataSource;
import org.apache.arrow.driver.jdbc.utils.BaseProperty;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.OutboundStreamListener;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * Utility class for unit tests that need to instantiate a {@link FlightServer}
 * and interact with it.
 */
public class FlightServerTestRule implements TestRule, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightServerTestRule.class);

  static final String QUERY_STRING = "SELECT * FROM TEST";
  private static final List<String> QUERY_TICKETS = ImmutableList.of(
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(),
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(),
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString(),
      UUID.randomUUID().toString());

  static final String METADATA_QUERY_STRING = "SELECT * FROM METADATA";
  private static final List<String> METADATA_QUERY_TICKETS = ImmutableList.of(
      UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());

  private final Map<BaseProperty, Object> properties;
  private final BufferAllocator allocator;
  private final BasicDataSource dataSource;

  FlightServerTestRule(Map<BaseProperty, Object> properties) {
    this(properties, new RootAllocator(Long.MAX_VALUE));
  }

  private FlightServerTestRule(Map<BaseProperty, Object> properties, BufferAllocator allocator) {
    this.properties = generateDefaults();
    this.properties.putAll(properties);

    this.allocator = allocator;

    Properties dataSourceProperties = new Properties();
    dataSourceProperties.put(USERNAME.getEntry().getKey(), getProperty(USERNAME));
    dataSourceProperties.put(PASSWORD.getEntry().getKey(), getProperty(PASSWORD));

    final String url = "jdbc:arrow-flight://" + getProperty(HOST) + ":" + getProperty(PORT);
    this.dataSource = new ArrowFlightJdbcDataSource(url, dataSourceProperties);
  }

  /**
   * Get the {@code Object} mapped to the provided {@link BaseProperty}.
   *
   * @param property the key with which to find the {@code Object} mapped to it.
   * @return the {@code Object} mapped to the provided {@code BaseProperty}.
   */
  private Object getProperty(BaseProperty property) {
    return properties.get(property);
  }

  /**
   * Gets the {@link Properties} of the {@link FlightServer} managed by this {@link TestRule}.
   * <p>This should be used in tests that actually need to connect to the {@link FlightServer}
   * using valid {@code Properties}.
   *
   * @return the properties of the {@code FlightServer} managed by this {@code TestRule}.
   */
  public Properties getProperties() {
    // TODO Implement this.
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  /**
   * Get a connection with the server to be used within the test.
   *
   * @return a valid JDBC connection.
   * @throws SQLException in case of error.
   */
  Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try (FlightServer flightServer =
                 getStartServer(location -> FlightServer.builder(allocator, location, getFlightProducer())
                     .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                         new BasicCallHeaderAuthenticator(FlightServerTestRule.this::validate)))
                     .build(), 3)) {
          LOGGER.info("Started " + FlightServer.class.getName() + " as " + flightServer);
          base.evaluate();
        } finally {
          close();
        }
      }
    };
  }

  private FlightServer getStartServer(Function<Location, FlightServer> newServerFromLocation, int retries)
      throws IOException {

    final Deque<ReflectiveOperationException> exceptions = new ArrayDeque<>();

    for (; retries > 0; retries--) {
      final Location location =
          Location.forGrpcInsecure((String) getProperty(BaseProperty.HOST), (int) getProperty(BaseProperty.PORT));
      final FlightServer server = newServerFromLocation.apply(location);
      try {
        Method start = server.getClass().getMethod("start");
        start.setAccessible(true);
        start.invoke(server);
        return server;
      } catch (ReflectiveOperationException e) {
        exceptions.add(e);
      }
    }

    exceptions.forEach(e -> LOGGER.error("Failed to start a new " + FlightServer.class.getName() + ".", e));
    throw new IOException(exceptions.pop().getCause());
  }

  private FlightProducer getFlightProducer() {
    return new FlightProducer() {
      @Override
      public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener listener) {
        checkUsername(callContext, listener);

        final Random random = new Random(10);

        final Schema querySchema = new Schema(ImmutableList.of(
            new Field("ID", new FieldType(true, new ArrowType.Int(64, true), null), null),
            new Field("Name", new FieldType(true, new ArrowType.Utf8(), null), null),
            new Field("Age", new FieldType(true, new ArrowType.Int(32, false), null), null),
            new Field("Salary", new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null),
                null),
            new Field("Hire Date", new FieldType(true, new ArrowType.Date(DateUnit.DAY), null), null),
            new Field("Last Sale", new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null),
                null)
        ));

        final Schema metadataSchema = new Schema(ImmutableList.of(
            new Field("integer0", new FieldType(true, new ArrowType.Int(64, true), null), null),
            new Field("string1", new FieldType(true, new ArrowType.Utf8(), null), null),
            new Field("float2", new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null),
                null)
        ));

        String ticketString = new String(ticket.getBytes(), StandardCharsets.UTF_8);
        if (QUERY_TICKETS.contains(ticketString)) {
          final int rowsPerPage = 5000;
          final int page = QUERY_TICKETS.indexOf(ticketString);

          try (final VectorSchemaRoot root = VectorSchemaRoot.create(querySchema, allocator)) {
            root.allocateNew();
            listener.start(root);
            int batchSize = 500;
            int indexOnBatch = 0;

            int resultsOffset = page * rowsPerPage;
            for (int i = 0; i < rowsPerPage; i++) {
              ((BigIntVector) root.getVector("ID")).setSafe(indexOnBatch, random.nextLong());
              ((VarCharVector) root.getVector("Name"))
                  .setSafe(indexOnBatch, new Text("Test Name #" + (resultsOffset + i)));
              ((UInt4Vector) root.getVector("Age")).setSafe(indexOnBatch, random.nextInt(Integer.MAX_VALUE));
              ((Float8Vector) root.getVector("Salary")).setSafe(indexOnBatch, random.nextDouble());
              ((DateDayVector) root.getVector("Hire Date")).setSafe(indexOnBatch, random.nextInt(Integer.MAX_VALUE));
              ((TimeStampMilliVector) root.getVector("Last Sale")).setSafe(indexOnBatch, Instant.now().toEpochMilli());

              indexOnBatch++;
              if (indexOnBatch == batchSize) {
                root.setRowCount(indexOnBatch);

                if (listener.isCancelled()) {
                  return;
                }

                listener.putNext();
                root.allocateNew();
                indexOnBatch = 0;
              }
            }
            if (listener.isCancelled()) {
              return;
            }

            root.setRowCount(indexOnBatch);
            listener.putNext();
          } finally {
            listener.completed();
          }
        } else if (METADATA_QUERY_TICKETS.contains(ticketString)) {
          try (final VectorSchemaRoot root = VectorSchemaRoot.create(metadataSchema, allocator)) {
            root.allocateNew();
            ((BigIntVector) root.getVector("integer0")).setSafe(0, 1);
            ((VarCharVector) root.getVector("string1")).setSafe(0, new Text("teste"));
            ((Float4Vector) root.getVector("float2")).setSafe(0, (float) 4.1);
            root.setRowCount(1);
            listener.start(root);
            listener.putNext();
          } finally {
            listener.completed();
          }
        } else {
          throw new RuntimeException();
        }
      }

      @Override
      public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
        checkUsername(callContext, streamListener);
      }

      @Override
      public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
        try {
          Method toProtocol = Location.class.getDeclaredMethod("toProtocol");
          toProtocol.setAccessible(true);
          Flight.Location location = (Flight.Location) toProtocol.invoke(new Location("grpc+tcp://localhost"));

          final String commandString = new String(flightDescriptor.getCommand(), StandardCharsets.UTF_8);

          final Flight.FlightInfo.Builder flightInfoBuilder = Flight.FlightInfo.newBuilder()
              .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
                  .setType(Flight.FlightDescriptor.DescriptorType.CMD)
                  .setCmd(ByteString.copyFrom(commandString, StandardCharsets.UTF_8)));

          if (commandString.equals(QUERY_STRING)) {
            QUERY_TICKETS.forEach(ticket -> {
              final byte[] ticketBytes = ticket.getBytes(StandardCharsets.UTF_8);
              flightInfoBuilder.addEndpoint(Flight.FlightEndpoint.newBuilder()
                  .addLocation(location)
                  .setTicket(Flight.Ticket.newBuilder().setTicket(ByteString.copyFrom(ticketBytes)).build()));
            });
          } else if (commandString.equals(METADATA_QUERY_STRING)) {
            METADATA_QUERY_TICKETS.forEach(ticket -> {
              final byte[] ticketBytes = ticket.getBytes(StandardCharsets.UTF_8);
              flightInfoBuilder.addEndpoint(Flight.FlightEndpoint.newBuilder()
                  .addLocation(location)
                  .setTicket(Flight.Ticket.newBuilder().setTicket(ByteString.copyFrom(ticketBytes)).build()));
            });
          } else {
            throw new SQLException("Invalid query");
          }

          Constructor<FlightInfo> constructor = FlightInfo.class
              .getDeclaredConstructor(org.apache.arrow.flight.impl.Flight.FlightInfo.class);
          constructor.setAccessible(true);
          return constructor.newInstance(flightInfoBuilder.build());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Runnable acceptPut(CallContext callContext, FlightStream flightStream,
                                StreamListener<PutResult> streamListener) {
        // TODO Implement this.
        return null;
      }

      @Override
      public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
        // TODO Implement this.
      }

      @Override
      public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
        // TODO Implement this.
      }

      private void checkUsername(CallContext context, StreamListener<?> listener) {
        if (context.peerIdentity().equals(getProperty(BaseProperty.USERNAME))) {
          listener.onCompleted();
          return;
        }
        listener.onError(new IllegalArgumentException("Invalid username."));
      }

      private void checkUsername(CallContext context, OutboundStreamListener listener) {
        if (!context.peerIdentity().equals(getProperty(BaseProperty.USERNAME))) {
          listener.error(new IllegalArgumentException("Invalid username."));
        }
      }
    };
  }

  private CallHeaderAuthenticator.AuthResult validate(final String username,
                                                      final String password) {
    if ((getProperty(BaseProperty.USERNAME)).equals(Preconditions.checkNotNull(username)) &&
        (getProperty(BaseProperty.PASSWORD)).equals(Preconditions.checkNotNull(password))) {
      return () -> username;
    }

    throw CallStatus.UNAUTHENTICATED.withDescription("Invalid credentials.").toRuntimeException();
  }

  private Map<BaseProperty, Object> generateDefaults() {
    final Map<BaseProperty, Object> propertiesMap = new HashMap<>();
    Arrays.stream(BaseProperty.values()).forEach(property -> {
      propertiesMap.put(property, property.getEntry().getValue());
    });
    return propertiesMap;
  }

  @Override
  public void close() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator);

    dataSource.close();
  }
}

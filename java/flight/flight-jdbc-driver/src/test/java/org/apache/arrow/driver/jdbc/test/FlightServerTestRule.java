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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

/**
 * Utility class for unit tests that need to instantiate a {@link FlightServer}
 * and interact with it.
 */
public class FlightServerTestRule implements TestRule, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FlightServerTestRule.class);
  private static final byte[] QUERY_TICKET = "SELECT * FROM TEST".getBytes(StandardCharsets.UTF_8);

  private final Map<BaseProperty, Object> properties;
  private final BufferAllocator allocator;

  public FlightServerTestRule(Map<BaseProperty, Object> properties) {
    this();
    this.properties.putAll(properties);
  }

  public FlightServerTestRule(BufferAllocator allocator) {
    properties = generateDefaults();
    this.allocator = allocator;
  }

  public FlightServerTestRule() {
    this(new RootAllocator(Long.MAX_VALUE));
  }

  /**
   * Get the {@code Object} mapped to the provided {@link BaseProperty}.
   *
   * @param property the key with which to find the {@code Object} mapped to it.
   * @return the {@code Object} mapped to the provided {@code BaseProperty}.
   */
  public Object getProperty(BaseProperty property) {
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

        if (Arrays.equals(QUERY_TICKET, ticket.getBytes())) {
          final List<FieldVector> vectors = new ArrayList<>();
          for (int col = 0; col < 4; col++) {
            final BigIntVector vector = new BigIntVector("f" + col, allocator);
            for (int row = 0; row < 10; row++) {
              vector.setSafe(row, row);
            }
            vectors.add(vector);
          }
          try (final VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
            root.setRowCount(10);
            listener.start(root);
            listener.putNext();
            listener.putNext();
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

          final byte[] value = flightDescriptor.getCommand();

          Flight.FlightInfo getInfo = Flight.FlightInfo.newBuilder()
              .setFlightDescriptor(Flight.FlightDescriptor.newBuilder()
                  .setType(Flight.FlightDescriptor.DescriptorType.CMD)
                  .setCmd(ByteString.copyFrom(value)))
              .addEndpoint(Flight.FlightEndpoint.newBuilder()
                  .addLocation(location)
                  .setTicket(Flight.Ticket.newBuilder()
                      .setTicket(ByteString.copyFrom(value))
                      .build())
              )
              .build();

          Constructor<FlightInfo> constructor = FlightInfo.class
              .getDeclaredConstructor(org.apache.arrow.flight.impl.Flight.FlightInfo.class);
          constructor.setAccessible(true);
          return constructor.newInstance(getInfo);
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
  }
}

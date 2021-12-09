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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Test an edge case in middleware: gRPC-Java consolidates headers and trailers if a call fails immediately. On the
 * gRPC implementation side, we need to watch for this, or else we'll have a call with "no headers" if we only look
 * for headers.
 */
final class MiddlewareScenario implements Scenario {

  private static final String HEADER = "x-middleware";
  private static final String EXPECTED_HEADER_VALUE = "expected value";
  private static final byte[] COMMAND_SUCCESS = "success".getBytes(StandardCharsets.UTF_8);

  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) {
    return new NoOpFlightProducer() {
      @Override
      public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        if (descriptor.isCommand()) {
          if (Arrays.equals(COMMAND_SUCCESS, descriptor.getCommand())) {
            return new FlightInfo(new Schema(Collections.emptyList()), descriptor, Collections.emptyList(), -1, -1);
          }
        }
        throw CallStatus.UNIMPLEMENTED.toRuntimeException();
      }
    };
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
    builder.middleware(FlightServerMiddleware.Key.of("test"), new InjectingServerMiddleware.Factory());
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient ignored) throws Exception {
    final ExtractingClientMiddleware.Factory factory = new ExtractingClientMiddleware.Factory();
    try (final FlightClient client = FlightClient.builder(allocator, location).intercept(factory).build()) {
      // Should fail immediately
      IntegrationAssertions.assertThrows(FlightRuntimeException.class,
          () -> client.getInfo(FlightDescriptor.command(new byte[0])));
      if (!EXPECTED_HEADER_VALUE.equals(factory.extractedHeader)) {
        throw new AssertionError(
            "Expected to extract the header value '" +
                EXPECTED_HEADER_VALUE +
                "', but found: " +
                factory.extractedHeader);
      }

      // Should not fail
      factory.extractedHeader = "";
      client.getInfo(FlightDescriptor.command(COMMAND_SUCCESS));
      if (!EXPECTED_HEADER_VALUE.equals(factory.extractedHeader)) {
        throw new AssertionError(
            "Expected to extract the header value '" +
                EXPECTED_HEADER_VALUE +
                "', but found: " +
                factory.extractedHeader);
      }
    }
  }

  /** Middleware that inserts a constant value in outgoing requests. */
  static class InjectingServerMiddleware implements FlightServerMiddleware {

    private final String headerValue;

    InjectingServerMiddleware(String incoming) {
      this.headerValue = incoming;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.insert("x-middleware", headerValue);
    }

    @Override
    public void onCallCompleted(CallStatus status) {
    }

    @Override
    public void onCallErrored(Throwable err) {
    }

    /** The factory for the server middleware. */
    static class Factory implements FlightServerMiddleware.Factory<InjectingServerMiddleware> {

      @Override
      public InjectingServerMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders,
          RequestContext context) {
        String incoming = incomingHeaders.get(HEADER);
        return new InjectingServerMiddleware(incoming == null ? "" : incoming);
      }
    }
  }

  /** Middleware that pulls a value out of incoming responses. */
  static class ExtractingClientMiddleware implements FlightClientMiddleware {

    private final ExtractingClientMiddleware.Factory factory;

    public ExtractingClientMiddleware(ExtractingClientMiddleware.Factory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.insert(HEADER, EXPECTED_HEADER_VALUE);
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
      this.factory.extractedHeader = incomingHeaders.get(HEADER);
    }

    @Override
    public void onCallCompleted(CallStatus status) {
    }

    /** The factory for the client middleware. */
    static class Factory implements FlightClientMiddleware.Factory {

      String extractedHeader = null;

      @Override
      public FlightClientMiddleware onCallStarted(CallInfo info) {
        return new ExtractingClientMiddleware(this);
      }
    }
  }
}

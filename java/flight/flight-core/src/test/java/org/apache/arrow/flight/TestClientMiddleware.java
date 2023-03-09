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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * A basic test of client middleware using a simplified OpenTracing-like example.
 */
public class TestClientMiddleware {

  /**
   * Test that a client middleware can fail a call before it starts by throwing a {@link FlightRuntimeException}.
   */
  @Test
  public void clientMiddleware_failCallBeforeSending() {
    test(new NoOpFlightProducer(), null, Collections.singletonList(new CallRejector.Factory()),
        (allocator, client) -> {
          FlightTestUtil.assertCode(FlightStatusCode.UNAVAILABLE, client::listActions);
        });
  }

  /**
   * Test an OpenTracing-like scenario where client and server middleware work together to propagate a request ID
   * without explicit intervention from the service implementation.
   */
  @Test
  public void middleware_propagateHeader() {
    final Context context = new Context("span id");
    test(new NoOpFlightProducer(),
        new TestServerMiddleware.ServerMiddlewarePair<>(
            FlightServerMiddleware.Key.of("test"), new ServerSpanInjector.Factory()),
        Collections.singletonList(new ClientSpanInjector.Factory(context)),
        (allocator, client) -> {
          FlightTestUtil.assertCode(FlightStatusCode.UNIMPLEMENTED, () -> client.listActions().forEach(actionType -> {
          }));
        });
    Assertions.assertEquals(context.outgoingSpanId, context.incomingSpanId);
    Assertions.assertNotNull(context.finalStatus);
    Assertions.assertEquals(FlightStatusCode.UNIMPLEMENTED, context.finalStatus.code());
  }

  /** Ensure both server and client can send and receive multi-valued headers (both binary and text values). */
  @Test
  public void testMultiValuedHeaders() {
    final MultiHeaderClientMiddlewareFactory clientFactory = new MultiHeaderClientMiddlewareFactory();
    test(new NoOpFlightProducer(),
        new TestServerMiddleware.ServerMiddlewarePair<>(
            FlightServerMiddleware.Key.of("test"), new MultiHeaderServerMiddlewareFactory()),
        Collections.singletonList(clientFactory),
        (allocator, client) -> {
          FlightTestUtil.assertCode(FlightStatusCode.UNIMPLEMENTED, () -> client.listActions().forEach(actionType -> {
          }));
        });
    // The server echoes the headers we send back to us, so ensure all the ones we sent are present with the correct
    // values in the correct order.
    for (final Map.Entry<String, List<byte[]>> entry : EXPECTED_BINARY_HEADERS.entrySet()) {
      // Compare header values entry-by-entry because byte arrays don't compare via equals
      final List<byte[]> receivedValues = clientFactory.lastBinaryHeaders.get(entry.getKey());
      Assertions.assertNotNull(receivedValues, "Missing for header: " + entry.getKey());
      Assertions.assertEquals(
          entry.getValue().size(),
          receivedValues.size(), "Missing or wrong value for header: " + entry.getKey());
      for (int i = 0; i < entry.getValue().size(); i++) {
        Assertions.assertArrayEquals(entry.getValue().get(i), receivedValues.get(i));
      }
    }
    for (final Map.Entry<String, List<String>> entry : EXPECTED_TEXT_HEADERS.entrySet()) {
      Assertions.assertEquals(
          entry.getValue(),
          clientFactory.lastTextHeaders.get(entry.getKey()),
          "Missing or wrong value for header: " + entry.getKey()
      );
    }
  }

  private static <T extends FlightServerMiddleware> void test(FlightProducer producer,
      TestServerMiddleware.ServerMiddlewarePair<T> serverMiddleware,
      List<FlightClientMiddleware.Factory> clientMiddleware,
      BiConsumer<BufferAllocator, FlightClient> body) {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final FlightServer.Builder serverBuilder =
          FlightServer.builder(allocator, forGrpcInsecure(LOCALHOST, 0), producer);
      if (serverMiddleware != null) {
        serverBuilder.middleware(serverMiddleware.key, serverMiddleware.factory);
      }
      final FlightServer server = serverBuilder.build().start();

      FlightClient.Builder clientBuilder = FlightClient.builder(allocator, server.getLocation());
      clientMiddleware.forEach(clientBuilder::intercept);
      try (final FlightServer ignored = server;
           final FlightClient client = clientBuilder.build()
      ) {
        body.accept(allocator, client);
      }
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A server middleware component that reads a request ID from incoming headers and sends the request ID back on
   * outgoing headers.
   */
  static class ServerSpanInjector implements FlightServerMiddleware {

    private final String spanId;

    public ServerSpanInjector(String spanId) {
      this.spanId = spanId;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.insert("x-span", spanId);
    }

    @Override
    public void onCallCompleted(CallStatus status) {

    }

    @Override
    public void onCallErrored(Throwable err) {

    }

    static class Factory implements FlightServerMiddleware.Factory<ServerSpanInjector> {

      @Override
      public ServerSpanInjector onCallStarted(CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
        return new ServerSpanInjector(incomingHeaders.get("x-span"));
      }
    }
  }

  /**
   * A client middleware component that, given a mock OpenTracing-like "request context", sends the request ID in the
   * context on outgoing headers and reads it from incoming headers.
   */
  static class ClientSpanInjector implements FlightClientMiddleware {

    private final Context context;

    public ClientSpanInjector(Context context) {
      this.context = context;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.insert("x-span", context.outgoingSpanId);
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
      context.incomingSpanId = incomingHeaders.get("x-span");
    }

    @Override
    public void onCallCompleted(CallStatus status) {
      context.finalStatus = status;
    }

    static class Factory implements FlightClientMiddleware.Factory {

      private final Context context;

      Factory(Context context) {
        this.context = context;
      }

      @Override
      public FlightClientMiddleware onCallStarted(CallInfo info) {
        return new ClientSpanInjector(context);
      }
    }
  }

  /**
   * A mock OpenTracing-like "request context".
   */
  static class Context {

    final String outgoingSpanId;
    String incomingSpanId;
    CallStatus finalStatus;

    Context(String spanId) {
      this.outgoingSpanId = spanId;
    }
  }

  /**
   * A client middleware that fails outgoing calls.
   */
  static class CallRejector implements FlightClientMiddleware {

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
    }

    @Override
    public void onCallCompleted(CallStatus status) {
    }

    static class Factory implements FlightClientMiddleware.Factory {

      @Override
      public FlightClientMiddleware onCallStarted(CallInfo info) {
        throw CallStatus.UNAVAILABLE.withDescription("Rejecting call.").toRuntimeException();
      }
    }
  }

  // Used to test that middleware can send and receive multi-valued text and binary headers.
  static Map<String, List<byte[]>> EXPECTED_BINARY_HEADERS = new HashMap<String, List<byte[]>>();
  static Map<String, List<String>> EXPECTED_TEXT_HEADERS = new HashMap<String, List<String>>();

  static {
    EXPECTED_BINARY_HEADERS.put("x-binary-bin", Arrays.asList(new byte[] {0}, new byte[]{1}));
    EXPECTED_TEXT_HEADERS.put("x-text", Arrays.asList("foo", "bar"));
  }

  static class MultiHeaderServerMiddlewareFactory implements
      FlightServerMiddleware.Factory<MultiHeaderServerMiddleware> {
    @Override
    public MultiHeaderServerMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders,
        RequestContext context) {
      // Echo the headers back to the client. Copy values out of CallHeaders since the underlying gRPC metadata
      // object isn't safe to use after this function returns.
      Map<String, List<byte[]>> binaryHeaders = new HashMap<>();
      Map<String, List<String>> textHeaders = new HashMap<>();
      for (final String key : incomingHeaders.keys()) {
        if (key.endsWith("-bin")) {
          binaryHeaders.compute(key, (ignored, values) -> {
            if (values == null) {
              values = new ArrayList<>();
            }
            incomingHeaders.getAllByte(key).forEach(values::add);
            return values;
          });
        } else {
          textHeaders.compute(key, (ignored, values) -> {
            if (values == null) {
              values = new ArrayList<>();
            }
            incomingHeaders.getAll(key).forEach(values::add);
            return values;
          });
        }
      }
      return new MultiHeaderServerMiddleware(binaryHeaders, textHeaders);
    }
  }

  static class MultiHeaderServerMiddleware implements FlightServerMiddleware {
    private final Map<String, List<byte[]>> binaryHeaders;
    private final Map<String, List<String>> textHeaders;

    MultiHeaderServerMiddleware(Map<String, List<byte[]>> binaryHeaders, Map<String, List<String>> textHeaders) {
      this.binaryHeaders = binaryHeaders;
      this.textHeaders = textHeaders;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      binaryHeaders.forEach((key, values) -> values.forEach(value -> outgoingHeaders.insert(key, value)));
      textHeaders.forEach((key, values) -> values.forEach(value -> outgoingHeaders.insert(key, value)));
    }

    @Override
    public void onCallCompleted(CallStatus status) {}

    @Override
    public void onCallErrored(Throwable err) {}
  }

  static class MultiHeaderClientMiddlewareFactory implements FlightClientMiddleware.Factory {
    Map<String, List<byte[]>> lastBinaryHeaders = null;
    Map<String, List<String>> lastTextHeaders = null;

    @Override
    public FlightClientMiddleware onCallStarted(CallInfo info) {
      return new MultiHeaderClientMiddleware(this);
    }
  }

  static class MultiHeaderClientMiddleware implements FlightClientMiddleware {
    private final MultiHeaderClientMiddlewareFactory factory;

    public MultiHeaderClientMiddleware(MultiHeaderClientMiddlewareFactory factory) {
      this.factory = factory;
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
      for (final Map.Entry<String, List<byte[]>> entry : EXPECTED_BINARY_HEADERS.entrySet()) {
        entry.getValue().forEach((value) -> outgoingHeaders.insert(entry.getKey(), value));
        Assertions.assertTrue(outgoingHeaders.containsKey(entry.getKey()));
      }
      for (final Map.Entry<String, List<String>> entry : EXPECTED_TEXT_HEADERS.entrySet()) {
        entry.getValue().forEach((value) -> outgoingHeaders.insert(entry.getKey(), value));
        Assertions.assertTrue(outgoingHeaders.containsKey(entry.getKey()));
      }
    }

    @Override
    public void onHeadersReceived(CallHeaders incomingHeaders) {
      factory.lastBinaryHeaders = new HashMap<>();
      factory.lastTextHeaders = new HashMap<>();
      incomingHeaders.keys().forEach(header -> {
        if (header.endsWith("-bin")) {
          final List<byte[]> values = new ArrayList<>();
          incomingHeaders.getAllByte(header).forEach(values::add);
          factory.lastBinaryHeaders.put(header, values);
        } else {
          final List<String> values = new ArrayList<>();
          incomingHeaders.getAll(header).forEach(values::add);
          factory.lastTextHeaders.put(header, values);
        }
      });
    }

    @Override
    public void onCallCompleted(CallStatus status) {}
  }
}

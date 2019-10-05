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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * A basic test of client middleware using a simplified OpenTracing-like example.
 */
@RunWith(JUnit4.class)
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
    Assert.assertEquals(context.outgoingSpanId, context.incomingSpanId);
    Assert.assertNotNull(context.finalStatus);
    Assert.assertEquals(FlightStatusCode.UNIMPLEMENTED, context.finalStatus.code());
  }

  private static <T extends FlightServerMiddleware> void test(FlightProducer producer,
      TestServerMiddleware.ServerMiddlewarePair<T> serverMiddleware,
      List<FlightClientMiddleware.Factory> clientMiddleware,
      BiConsumer<BufferAllocator, FlightClient> body) {
    try (final BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
      final FlightServer server = FlightTestUtil
          .getStartedServer(location -> {
            final FlightServer.Builder builder = FlightServer.builder(allocator, location, producer);
            if (serverMiddleware != null) {
              builder.middleware(serverMiddleware.key, serverMiddleware.factory);
            }
            return builder.build();
          });
      FlightClient.Builder builder = FlightClient.builder(allocator, server.getLocation());
      clientMiddleware.forEach(builder::intercept);
      try (final FlightServer ignored = server;
          final FlightClient client = builder.build()
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
      public ServerSpanInjector onCallStarted(CallInfo info, CallHeaders incomingHeaders) {
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
}

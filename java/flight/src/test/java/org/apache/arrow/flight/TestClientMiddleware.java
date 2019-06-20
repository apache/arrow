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

  @Test
  public void rejectCall() {
    test(new Producer(), null, Collections.singletonList(new CallRejector.Factory()),
        (allocator, client) -> {
          FlightTestUtil.assertCode(FlightStatusCode.UNAVAILABLE, client::listActions);
        });
  }

  @Test
  public void injectSpans() {
    final Context context = new Context("span id");
    test(new Producer(),
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

  private static class Producer extends NoOpFlightProducer {
  }

  static class ServerSpanInjector implements FlightServerMiddleware {

    private final String spanId;

    public ServerSpanInjector(String spanId) {
      this.spanId = spanId;
    }

    @Override
    public void sendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.putText("x-span", spanId);
    }

    @Override
    public void callCompleted(CallStatus status) {

    }

    @Override
    public void callErrored(Throwable err) {

    }

    static class Factory implements FlightServerMiddleware.Factory<ServerSpanInjector> {

      @Override
      public ServerSpanInjector startCall(CallInfo info, CallHeaders incomingHeaders) {
        return new ServerSpanInjector(incomingHeaders.getText("x-span"));
      }
    }
  }

  static class ClientSpanInjector implements FlightClientMiddleware {

    private final Context context;

    public ClientSpanInjector(Context context) {
      this.context = context;
    }

    @Override
    public void sendingHeaders(CallHeaders outgoingHeaders) {
      outgoingHeaders.putText("x-span", context.outgoingSpanId);
    }

    @Override
    public void headersReceived(CallHeaders incomingHeaders) {
      context.incomingSpanId = incomingHeaders.getText("x-span");
    }

    @Override
    public void callCompleted(CallStatus status) {
      context.finalStatus = status;
    }

    static class Factory implements FlightClientMiddleware.Factory {

      private final Context context;

      Factory(Context context) {
        this.context = context;
      }

      @Override
      public FlightClientMiddleware startCall(CallInfo info) {
        return new ClientSpanInjector(context);
      }
    }
  }

  static class Context {

    public final String outgoingSpanId;
    public String incomingSpanId;
    public CallStatus finalStatus;

    Context(String spanId) {
      this.outgoingSpanId = spanId;
    }
  }

  static class CallRejector implements FlightClientMiddleware {

    @Override
    public void sendingHeaders(CallHeaders outgoingHeaders) {
    }

    @Override
    public void headersReceived(CallHeaders incomingHeaders) {
    }

    @Override
    public void callCompleted(CallStatus status) {
    }

    static class Factory implements FlightClientMiddleware.Factory {

      @Override
      public FlightClientMiddleware startCall(CallInfo info) {
        throw CallStatus.UNAVAILABLE.withDescription("Rejecting call.").toRuntimeException();
      }
    }
  }
}

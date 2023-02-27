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

package org.apache.arrow.flight.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightMethod;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.FlightTestUtil;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.flight.SyncPutListener;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;


/**
 * Tests to ensure custom headers are passed along to the server for each command.
 */
public class CustomHeaderTest {
  FlightServer server;
  FlightClient client;
  BufferAllocator allocator;
  TestCustomHeaderMiddleware.Factory headersMiddleware;
  HeaderCallOption headers;
  Map<String, String> testHeaders = ImmutableMap.of(
          "foo", "bar",
          "bar", "foo",
          "answer", "42"
  );

  @Before
  public void setUp() throws Exception {
    allocator = new RootAllocator(Integer.MAX_VALUE);
    headersMiddleware = new TestCustomHeaderMiddleware.Factory();
    FlightCallHeaders callHeaders = new FlightCallHeaders();
    for (Map.Entry<String, String> entry : testHeaders.entrySet()) {
      callHeaders.insert(entry.getKey(), entry.getValue());
    }
    headers = new HeaderCallOption(callHeaders);
    server = FlightServer.builder(allocator,
            Location.forGrpcInsecure(FlightTestUtil.LOCALHOST, /*port*/ 0),
            new NoOpFlightProducer())
        .middleware(FlightServerMiddleware.Key.of("customHeader"), headersMiddleware)
        .build();
    server.start();
    client = FlightClient.builder(allocator, server.getLocation()).build();
  }

  @After
  public void tearDown() throws Exception {
    allocator.getChildAllocators().forEach(BufferAllocator::close);
    AutoCloseables.close(allocator, server, client);
  }

  @Test
  public void testHandshake() {
    try {
      client.handshake(headers);
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.HANDSHAKE);
  }

  @Test
  public void testGetSchema() {
    try {
      client.getSchema(FlightDescriptor.command(new byte[0]), headers);
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.GET_SCHEMA);
  }

  @Test
  public void testGetFlightInfo() {
    try {
      client.getInfo(FlightDescriptor.command(new byte[0]), headers);
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.GET_FLIGHT_INFO);
  }

  @Test
  public void testListActions() {
    try {
      client.listActions(headers).iterator().next();
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.LIST_ACTIONS);
  }

  @Test
  public void testListFlights() {
    try {
      client.listFlights(new Criteria(new byte[]{1}), headers).iterator().next();
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.LIST_FLIGHTS);
  }

  @Test
  public void testDoAction() {
    try {
      client.doAction(new Action("test"), headers).next();
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.DO_ACTION);
  }

  @Test
  public void testStartPut() {
    try {
      final ClientStreamListener listener = client.startPut(FlightDescriptor.command(new byte[0]),
              new SyncPutListener(),
              headers);
      listener.getResult();
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.DO_PUT);
  }

  @Test
  public void testGetStream() {
    try (final FlightStream stream = client.getStream(new Ticket(new byte[0]), headers)) {
      stream.next();
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.DO_GET);
  }

  @Test
  public void testDoExchange() {
    try (final FlightClient.ExchangeReaderWriter stream = client.doExchange(
            FlightDescriptor.command(new byte[0]),
            headers)
    ) {
      stream.getReader().next();
    } catch (Exception ignored) { }

    assertHeadersMatch(FlightMethod.DO_EXCHANGE);
  }

  private void assertHeadersMatch(FlightMethod method) {
    for (Map.Entry<String, String> entry : testHeaders.entrySet()) {
      Assert.assertEquals(entry.getValue(), headersMiddleware.getCustomHeader(method, entry.getKey()));
    }
  }

  /**
   * A middleware used to test if customHeaders are being sent to the server properly.
   */
  static class TestCustomHeaderMiddleware implements FlightServerMiddleware {

    public TestCustomHeaderMiddleware() {
    }

    @Override
    public void onBeforeSendingHeaders(CallHeaders callHeaders) {

    }

    @Override
    public void onCallCompleted(CallStatus callStatus) {

    }

    @Override
    public void onCallErrored(Throwable throwable) {

    }

    /**
     * A factory for the middleware that keeps track of the received headers and provides a way
     * to check those values for a given Flight Method.
     */
    static class Factory implements FlightServerMiddleware.Factory<TestCustomHeaderMiddleware> {
      private final Map<FlightMethod, CallHeaders> receivedCallHeaders = new HashMap<>();

      @Override
      public TestCustomHeaderMiddleware onCallStarted(CallInfo callInfo, CallHeaders callHeaders,
                                                      RequestContext requestContext) {

        receivedCallHeaders.put(callInfo.method(), callHeaders);
        return new TestCustomHeaderMiddleware();
      }

      public String getCustomHeader(FlightMethod method, String key) {
        CallHeaders headers = receivedCallHeaders.get(method);
        if (headers == null) {
          return null;
        }
        return headers.get(key);
      }
    }
  }   
}

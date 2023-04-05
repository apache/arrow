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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightClient.Builder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for TLS in Flight.
 */
public class TestTls {

  /**
   * Test a basic request over TLS.
   */
  @Test
  public void connectTls() {
    test((builder) -> {
      try (final InputStream roots = new FileInputStream(FlightTestUtil.exampleTlsRootCert().toFile());
          final FlightClient client = builder.trustedCertificates(roots).build()) {
        final Iterator<Result> responses = client.doAction(new Action("hello-world"));
        final byte[] response = responses.next().getBody();
        Assertions.assertEquals("Hello, world!", new String(response, StandardCharsets.UTF_8));
        Assertions.assertFalse(responses.hasNext());
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Make sure that connections are rejected when the root certificate isn't trusted.
   */
  @Test
  public void rejectInvalidCert() {
    test((builder) -> {
      try (final FlightClient client = builder.build()) {
        final Iterator<Result> responses = client.doAction(new Action("hello-world"));
        FlightTestUtil.assertCode(FlightStatusCode.UNAVAILABLE, () -> responses.next().getBody());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Make sure that connections are rejected when the hostname doesn't match.
   */
  @Test
  public void rejectHostname() {
    test((builder) -> {
      try (final InputStream roots = new FileInputStream(FlightTestUtil.exampleTlsRootCert().toFile());
          final FlightClient client = builder.trustedCertificates(roots).overrideHostname("fakehostname")
              .build()) {
        final Iterator<Result> responses = client.doAction(new Action("hello-world"));
        FlightTestUtil.assertCode(FlightStatusCode.UNAVAILABLE, () -> responses.next().getBody());
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Test a basic request over TLS.
   */
  @Test
  public void connectTlsDisableServerVerification() {
    test((builder) -> {
      try (final FlightClient client = builder.verifyServer(false).build()) {
        final Iterator<Result> responses = client.doAction(new Action("hello-world"));
        final byte[] response = responses.next().getBody();
        Assertions.assertEquals("Hello, world!", new String(response, StandardCharsets.UTF_8));
        Assertions.assertFalse(responses.hasNext());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  void test(Consumer<Builder> testFn) {
    final FlightTestUtil.CertKeyPair certKey = FlightTestUtil.exampleTlsCerts().get(0);
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer();
        FlightServer s = FlightServer.builder(a, forGrpcInsecure(LOCALHOST, 0), producer)
            .useTls(certKey.cert, certKey.key)
            .build().start()) {
      final Builder builder = FlightClient.builder(a, Location.forGrpcTls(FlightTestUtil.LOCALHOST, s.getPort()));
      testFn.accept(builder);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  static class Producer extends NoOpFlightProducer implements AutoCloseable {

    @Override
    public void doAction(CallContext context, Action action, StreamListener<Result> listener) {
      if (action.getType().equals("hello-world")) {
        listener.onNext(new Result("Hello, world!".getBytes(StandardCharsets.UTF_8)));
        listener.onCompleted();
        return;
      }
      listener
          .onError(CallStatus.UNIMPLEMENTED.withDescription("Invalid action " + action.getType()).toRuntimeException());
    }

    @Override
    public void close() {
    }
  }
}

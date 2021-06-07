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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;


public class FlightTestUtils {

  private static final Random RANDOM = new Random();

  private static final String LOCALHOST = "localhost";
  private static final String USERNAME_1 = "flight1";
  private static final String PASSWORD_1 = "woohoo1";
  private static final String USERNAME_INVALID = "bad";
  private static final String PASSWORD_INVALID = "wrong";
  private static final String USERNAME_2 = "flight2";
  private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);
  private static final String CONNECTION_PREFIX = "jdbc:arrow-flight://";

  public static String getConnectionPrefix() {
    return CONNECTION_PREFIX;
  }

  public static String getUsername1() {
    return USERNAME_1;
  }

  public static String getPassword1() {
    return PASSWORD_1;
  }

  public static String getUsernameInvalid() {
    return USERNAME_INVALID;
  }

  public static String getPasswordInvalid() {
    return PASSWORD_INVALID;
  }

  public static String getLocalhost() {
    return LOCALHOST;
  }

  public static BufferAllocator getAllocator() {
    return ALLOCATOR;
  }

  /**
   * Return a a FlightServer (actually anything that is startable)
   * that has been started bound to a random port.
   */
  public static <T> T getStartedServer(Function<Location, T> newServerFromLocation) throws IOException {
    IOException lastThrown = null;
    T server = null;
    for (int x = 0; x < 3; x++) {
      final int port = 49152 + RANDOM.nextInt(5000);
      final Location location = Location.forGrpcInsecure(LOCALHOST, port);
      lastThrown = null;
      try {
        server = newServerFromLocation.apply(location);
        try {
          server.getClass().getMethod("start").invoke(server);
        } catch (NoSuchMethodException | IllegalAccessException e) {
          throw new IllegalArgumentException("Couldn't call start method on object.", e);
        }
        break;
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof IOException) {
          lastThrown = (IOException) e.getTargetException();
        } else {
          throw (RuntimeException) e.getTargetException();
        }
      }
    }
    if (lastThrown != null) {
      throw lastThrown;
    }
    return server;
  }

  /**
   * Get a Flight Producer.
   *
   * @return NoOpFlightProducer.
   */
  public static FlightProducer getFlightProducer() {
    return new NoOpFlightProducer() {
      @Override
      public void listFlights(CallContext context, Criteria criteria,
                              StreamListener<FlightInfo> listener) {
        if (!context.peerIdentity().equals(USERNAME_1) && !context.peerIdentity().equals(USERNAME_2)) {
            listener.onError(new IllegalArgumentException("Invalid username"));
            return;
        }
        listener.onCompleted();
      }

      @Override
      public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        if (!context.peerIdentity().equals(USERNAME_1) && !context.peerIdentity().equals(USERNAME_2)) {
          listener.error(new IllegalArgumentException("Invalid username"));
          return;
        }
        final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a",
                Types.MinorType.BIGINT.getType())));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, ALLOCATOR)) {
          listener.start(root);
          root.allocateNew();
          root.setRowCount(4095);
          listener.putNext();
          listener.completed();
        }
      }
    };
  }


  /**
   * Get the Path from the Files to be used in the encrypted test of Flight.
   *
   * @return the Path from the Files with certificates and keys.
   */
  static Path getFlightTestDataRoot() {
    // #TODO Change this way to get Path
    return Paths.get("/home/jose/Documents/Dremio/arrow/testing/data/").resolve("flight");
  }

  /**
   * Create CertKeyPair object with the certificates and keys.
   *
   * @return A list with CertKeyPair.
   */
  public static List<CertKeyPair> exampleTlsCerts() {
    final Path root = getFlightTestDataRoot();
    return Arrays.asList(new CertKeyPair(root.resolve("cert0.pem").toFile(), root.resolve("cert0.pkcs1").toFile()),
            new CertKeyPair(root.resolve("cert1.pem").toFile(), root.resolve("cert1.pkcs1").toFile()));
  }

  public static class CertKeyPair {

    public final File cert;
    public final File key;

    public CertKeyPair(File cert, File key) {
      this.cert = cert;
      this.key = key;
    }
  }
}

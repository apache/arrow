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

package org.apache.arrow.driver.jdbc.test.utils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;

/**
 * Utility class for running tests against a FlightServer.
 *
 * @deprecated this class doesn't follow best practices
 * @see org.apache.arrow.driver.jdbc.test.FlightServerTestRule#apply
 */
@Deprecated
public final class FlightTestUtils {

  private static final Random RANDOM = new Random();

  private String url;
  private String username1;
  private String password1;
  private String usernameInvalid;
  private String passwordInvalid;
  private String connectionPrefix;

  public FlightTestUtils(String url, String username1, String password1,
                         String usernameInvalid, String passwordInvalid) {
    this.url = url;
    this.username1 = username1;
    this.password1 = password1;
    this.usernameInvalid = usernameInvalid;
    this.passwordInvalid = passwordInvalid;
    this.connectionPrefix = "jdbc:arrow-flight://";
  }

  public String getConnectionPrefix() {
    return connectionPrefix;
  }

  public String getUsername1() {
    return username1;
  }

  public String getPassword1() {
    return password1;
  }

  public String getUsernameInvalid() {
    return usernameInvalid;
  }

  public String getPasswordInvalid() {
    return passwordInvalid;
  }

  public String getUrl() {
    return url;
  }


  /**
   * Return a a FlightServer (actually anything that is startable)
   * that has been started bound to a random port.
   * @deprecated this approach is unnecessarily verbose and allows for little to no reuse.
   * @see org.apache.arrow.driver.jdbc.test.FlightServerTestRule
   */
  @Deprecated
  public <T> T getStartedServer(Function<Location, T> newServerFromLocation) throws IOException {
    IOException lastThrown = null;
    T server = null;
    for (int x = 0; x < 3; x++) {
      final int port = 49152 + RANDOM.nextInt(5000);
      final Location location = Location.forGrpcInsecure(this.url, port);
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
   * @deprecated this should not be visible.
   */
  @Deprecated
  public FlightProducer getFlightProducer(BufferAllocator allocator) {
    return new NoOpFlightProducer() {
      @Override
      public void listFlights(CallContext context, Criteria criteria,
                              StreamListener<FlightInfo> listener) {
        if (!context.peerIdentity().equals(username1)) {
            listener.onError(new IllegalArgumentException("Invalid username"));
            return;
        }
        listener.onCompleted();
      }

      @Override
      public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        if (!context.peerIdentity().equals(username1)) {
          listener.error(new IllegalArgumentException("Invalid username"));
          return;
        }
        final Schema pojoSchema = new Schema(ImmutableList.of(Field.nullable("a",
                Types.MinorType.BIGINT.getType())));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(pojoSchema, allocator)) {
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
  static Path getFlightTestDataRoot() throws URISyntaxException {
    return Paths.get(FlightTestUtils.class.getClassLoader().getResource("keys").toURI());
  }

  /**
   * Create CertKeyPair object with the certificates and keys.
   *
   * @return A list with CertKeyPair.
   */
  public static List<CertKeyPair> exampleTlsCerts() throws URISyntaxException {
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

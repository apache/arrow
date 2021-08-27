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

package org.apache.arrow.driver.jdbc.client.utils;

import static org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils.getCertificateStream;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

/**
 * Utility class for creating a client.
 */
public final class ClientCreationUtils {
  private ClientCreationUtils() {
    // Prevent instantiation.
  }

  /**
   * Instantiates a new {@link FlightClient} from the provided info.
   *
   * @param host                the host for the connection to be established.
   * @param port                the port for the connection to be established
   * @param keyStorePath        the keystore path for TLS encryption.
   * @param keyStorePassword    the keystore password for TLS encryption.
   * @param allocator           the {@link BufferAllocator} to use.
   * @param middlewareFactories the authentication middleware factory.
   * @param useTls              whether to use TLS encryption.
   * @return a new client associated to its call options.
   */
  public static FlightClient createNewClient(final String host, final int port,
                                             final String keyStorePath, final String keyStorePassword,
                                             final boolean useTls,
                                             final BufferAllocator allocator,
                                             final FlightClientMiddleware.Factory... middlewareFactories)
      throws GeneralSecurityException, IOException {
    return createNewClient(
        host, port, keyStorePath, keyStorePassword, useTls,
        allocator, Arrays.asList(middlewareFactories));
  }

  /**
   * Creates and get a new {@link FlightClient} and its {@link CallOption}s.
   *
   * @param host      the host.
   * @param port      the port.
   * @param username  the username.
   * @param password  the password.
   * @param allocator the {@link BufferAllocator}.
   * @param useTls    whether to use TLS encryption.
   * @param options   the {@code CallOption}s.
   * @return a new {@code FlightClient} and its {@code CallOption}s.
   * @throws GeneralSecurityException on error.
   * @throws IOException              on error.
   */
  public static Entry<FlightClient, CallOption[]> createAndGetClientInfo(final String host, final int port,
                                                                         final String username, final String password,
                                                                         final String keyStorePath,
                                                                         final String keyStorePassword,
                                                                         final BufferAllocator allocator,
                                                                         final boolean useTls,
                                                                         final CallOption... options)
      throws GeneralSecurityException, IOException {
    return createAndGetClientInfo(
        host, port, username, password,
        keyStorePath, keyStorePassword,
        allocator, useTls, Arrays.asList(options));
  }

  /**
   * Creates and get a new {@link FlightClient} and its {@link CallOption}s.
   *
   * @param host      the host.
   * @param port      the port.
   * @param username  the username.
   * @param password  the password.
   * @param allocator the {@link BufferAllocator}.
   * @param useTls    whether to use TLS encryption.
   * @param options   the {@code CallOption}s.
   * @return a new {@code FlightClient} and its {@code CallOption}s.
   * @throws GeneralSecurityException on error.
   * @throws IOException              on error.
   */
  public static Entry<FlightClient, CallOption[]> createAndGetClientInfo(final String host, final int port,
                                                                         final String username, final String password,
                                                                         final String keyStorePath,
                                                                         final String keyStorePassword,
                                                                         final BufferAllocator allocator,
                                                                         final boolean useTls,
                                                                         final Collection<CallOption> options)
      throws GeneralSecurityException, IOException {
    final Set<CallOption> theseOptions = new HashSet<>(options);
    final FlightClient client;
    if (username != null) {
      final ClientIncomingAuthHeaderMiddleware.Factory authFactory =
          new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
      client =
          ClientCreationUtils.createNewClient(
              host, port, keyStorePath, keyStorePassword, useTls, allocator, authFactory);
      theseOptions.add(ClientAuthenticationUtils.getAuthenticate(client, username, password, authFactory));
    } else {
      client = ClientCreationUtils.createNewClient(host, port, keyStorePath, keyStorePassword, useTls, allocator);
    }
    return new SimpleImmutableEntry<>(client, theseOptions.toArray(new CallOption[0]));
  }

  /**
   * Instantiates a new {@link FlightClient} from the provided info.
   *
   * @param host                the host for the connection to be established.
   * @param port                the port for the connection to be established.
   * @param keyStorePath        the keystore path for TLS encryption.
   * @param keyStorePassword    the keystore password for TLS encryption.
   * @param allocator           the {@link BufferAllocator} to use.
   * @param middlewareFactories the authentication middleware factory.
   * @param useTls              whether to use TLS encryption.
   * @return a new client associated to its call options.
   */
  public static FlightClient createNewClient(final String host, final int port,
                                             final String keyStorePath, final String keyStorePassword,
                                             final boolean useTls,
                                             final BufferAllocator allocator,
                                             final Collection<FlightClientMiddleware.Factory> middlewareFactories)
      throws GeneralSecurityException, IOException {
    Preconditions.checkNotNull(host, "Host cannot be null.");
    Preconditions.checkNotNull(allocator, "Allocator cannot be null.");
    Preconditions.checkNotNull(middlewareFactories, "Middleware factories cannot be null!.");
    FlightClient.Builder clientBuilder = FlightClient.builder().allocator(allocator);
    middlewareFactories.forEach(clientBuilder::intercept);
    Location location;
    if (useTls) {
      location = Location.forGrpcTls(host, port);
      clientBuilder.useTls();
    } else {
      location = Location.forGrpcInsecure(host, port);
    }
    clientBuilder.location(location);
    if (keyStorePath != null) {
      Preconditions.checkState(useTls, "KeyStore info cannot be provided when TLS encryption is disabled.");
      clientBuilder.trustedCertificates(getCertificateStream(keyStorePath, keyStorePassword));
    }
    return clientBuilder.build();
  }
}

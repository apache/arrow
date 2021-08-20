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

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import static org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils.getCertificateStream;

/**
 * Utility class for creating a client.
 */
public class ClientCreationUtils {
  private ClientCreationUtils() {
    // Prevent instantiation.
  }

  /**
   * Instantiates a new {@link FlightClient} from the provided info.
   *
   * @param address             the host and port for the connection to be established.
   * @param keyStoreInfo        the keystore path and keystore password for TLS encryption.
   * @param allocator           the {@link BufferAllocator} to use.
   * @param middlewareFactories the authentication middleware factory.
   * @param useTls              whether to use TLS encryption.
   * @return a new client associated to its call options.
   */
  public static FlightClient createNewClient(final Entry<String, Integer> address,
                                             final Entry<String, String> keyStoreInfo,
                                             final boolean useTls,
                                             final BufferAllocator allocator,
                                             final FlightClientMiddleware.Factory... middlewareFactories)
          throws GeneralSecurityException, IOException {
    return createNewClient(address, keyStoreInfo, useTls, allocator, Arrays.asList(middlewareFactories));
  }

  public static Entry<FlightClient, List<CallOption>> createAndGetClientInfo(final Entry<String, Integer> address,
                                                                             final Entry<String, String> credentials,
                                                                             final Entry<String, String> keyStoreInfo,
                                                                             final BufferAllocator allocator,
                                                                             final boolean useTls,
                                                                             final Collection<CallOption> options)
          throws GeneralSecurityException, IOException {
    final List<CallOption> theseOptions = new ArrayList<>(options);
    final FlightClient client;
    if (credentials != null) {
      final ClientIncomingAuthHeaderMiddleware.Factory authFactory =
              new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
      client = ClientCreationUtils.createNewClient(address, keyStoreInfo, useTls, allocator, authFactory);
      theseOptions.add(ClientAuthenticationUtils.getAuthenticate(client, credentials, authFactory));
    } else {
      client = ClientCreationUtils.createNewClient(address, keyStoreInfo, useTls, allocator);
    }
    return new SimpleImmutableEntry<>(client, theseOptions);
  }

  /**
   * Instantiates a new {@link FlightClient} from the provided info.
   *
   * @param address             the host and port for the connection to be established.
   * @param keyStoreInfo        the keystore path and keystore password for TLS encryption.
   * @param allocator           the {@link BufferAllocator} to use.
   * @param middlewareFactories the authentication middleware factory.
   * @param useTls              whether to use TLS encryption.
   * @return a new client associated to its call options.
   */
  public static FlightClient createNewClient(final Entry<String, Integer> address,
                                             final Entry<String, String> keyStoreInfo,
                                             final boolean useTls,
                                             final BufferAllocator allocator,
                                             final Collection<FlightClientMiddleware.Factory> middlewareFactories)
      throws GeneralSecurityException, IOException {
    Preconditions.checkNotNull(address, "Address cannot be null!");
    Preconditions.checkNotNull(allocator, "Allocator cannot be null!");
    Preconditions.checkNotNull(middlewareFactories, "Middleware factories cannot be null!");
    FlightClient.Builder clientBuilder = FlightClient.builder().allocator(allocator);
    middlewareFactories.forEach(clientBuilder::intercept);
    final String host = address.getKey();
    final int port = address.getValue();
    Location location;
    if (useTls) {
      location = Location.forGrpcTls(host, port);
      clientBuilder.useTls();
    } else {
      location = Location.forGrpcInsecure(host, port);
    }
    clientBuilder.location(location);
    if (keyStoreInfo != null) {
      Preconditions.checkState(useTls, "KeyStore info cannot be provided when TLS encryption is disabled.");
      clientBuilder.trustedCertificates(getCertificateStream(keyStoreInfo));
    }
    return clientBuilder.build();
  }
}

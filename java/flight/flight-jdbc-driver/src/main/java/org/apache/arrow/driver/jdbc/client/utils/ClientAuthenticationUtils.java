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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.auth2.BasicAuthCredentialWriter;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.util.Preconditions;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

/**
 * Utils for {@link FlightClientHandler} authentication.
 */
public final class ClientAuthenticationUtils {

  private ClientAuthenticationUtils() {
    // Prevent instantiation.
  }

  /**
   * Gets the {@link CredentialCallOption} for the provided authentication info.
   *
   * @param client   the client.
   * @param username the username.
   * @param password the password.
   * @param factory  the {@link ClientIncomingAuthHeaderMiddleware.Factory} to use.
   * @param options  the {@link CallOption}s to use.
   * @return the credential call option.
   */
  public static CredentialCallOption getAuthenticate(final FlightClient client,
                                                     final String username, final String password,
                                                     final ClientIncomingAuthHeaderMiddleware.Factory factory,
                                                     final CallOption... options) {

    return getAuthenticate(client,
        new CredentialCallOption(new BasicAuthCredentialWriter(username, password)),
        factory, options);
  }

  private static CredentialCallOption getAuthenticate(final FlightClient client,
                                                      final CredentialCallOption token,
                                                      final ClientIncomingAuthHeaderMiddleware.Factory factory,
                                                      final CallOption... options) {
    final List<CallOption> theseOptions = new ArrayList<>();
    theseOptions.add(token);
    theseOptions.addAll(Arrays.asList(options));
    client.handshake(theseOptions.toArray(new CallOption[0]));
    return factory.getCredentialCallOption();
  }

  /**
   * Generates an {@link InputStream} that contains certificates for a private
   * key.
   *
   * @param keyStorePath The path of the KeyStore.
   * @param keyStorePass The password of the KeyStore.
   * @return a new {code InputStream} containing the certificates.
   * @throws GeneralSecurityException on error.
   * @throws IOException              on error.
   */
  public static InputStream getCertificateStream(final String keyStorePath,
                                                 final String keyStorePass)
      throws GeneralSecurityException, IOException {
    Preconditions.checkNotNull(keyStorePath, "KeyStore path cannot be null!");
    Preconditions.checkNotNull(keyStorePass, "KeyStorePass cannot be null!");
    final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());

    try (final InputStream keyStoreStream = Files
        .newInputStream(Paths.get(Preconditions.checkNotNull(keyStorePath)))) {
      keyStore.load(keyStoreStream,
          Preconditions.checkNotNull(keyStorePass).toCharArray());
    }

    final Enumeration<String> aliases = keyStore.aliases();

    while (aliases.hasMoreElements()) {
      final String alias = aliases.nextElement();
      if (keyStore.isCertificateEntry(alias)) {
        return toInputStream(keyStore.getCertificate(alias));
      }
    }

    throw new CertificateException("Keystore did not have a certificate.");
  }

  private static InputStream toInputStream(final Certificate certificate)
      throws IOException {

    try (final StringWriter writer = new StringWriter();
         final JcaPEMWriter pemWriter = new JcaPEMWriter(writer)) {

      pemWriter.writeObject(certificate);
      pemWriter.flush();
      return new ByteArrayInputStream(
          writer.toString().getBytes(StandardCharsets.UTF_8));
    }
  }
}

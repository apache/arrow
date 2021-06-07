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

import com.google.common.base.Strings;
import org.apache.arrow.driver.jdbc.ArrowFlightClient;
import org.apache.arrow.flight.*;
import org.apache.arrow.flight.auth2.*;
import org.apache.calcite.avatica.org.apache.http.auth.UsernamePasswordCredentials;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;


public class ConnectionTlsTest {
    private FlightServer tlsServer;
    private static String serverUrl;


    @Before
    public void setUp() throws ClassNotFoundException, IOException {
        final FlightTestUtils.CertKeyPair certKey = FlightTestUtils.exampleTlsCerts().get(0);

        final FlightProducer flightProducer = FlightTestUtils.getFlightProducer();
        this.tlsServer = FlightTestUtils.getStartedServer(
                (location -> {
                    try {
                        return FlightServer
                                .builder(FlightTestUtils.getAllocator(), location, flightProducer)
                                .useTls(certKey.cert, certKey.key)
                                .headerAuthenticator(new GeneratedBearerTokenAuthenticator(
                                        new BasicCallHeaderAuthenticator(this::validate)))
                                .build();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
                ));
        this.serverUrl = FlightTestUtils.getConnectionPrefix() + FlightTestUtils.getLocalhost() + ":"
                + this.tlsServer.getPort();

        Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
    }

    /**
     * Validate the user's credential on a FlightServer.
     *
     * @param username flight server username.
     * @param password flight server password.
     * @return the result of validation.
     */
    private CallHeaderAuthenticator.AuthResult validate(String username, String password) {
        if (Strings.isNullOrEmpty(username)) {
            throw CallStatus.UNAUTHENTICATED.withDescription("Credentials not supplied.").toRuntimeException();
        }
        final String identity;
        if (FlightTestUtils.getUsername1().equals(username) &&
                FlightTestUtils.getPassword1().equals(password)) {
            identity = FlightTestUtils.getUsername1();
        } else {
            throw CallStatus.UNAUTHENTICATED.withDescription(
                    "Username or password is invalid.").toRuntimeException();
        }
        return () -> identity;
    }

    /**
     * Try to instantiate an encrypt FlightClient.
     *
     * @throws URISyntaxException on error.
     */
    @Test
    public void testGetEncryptedClient() throws URISyntaxException,
            KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {

        Properties properties = new Properties();

        properties.put("useTls", "true");
        properties.put("keyStorePath", "keyStore.jks");
        properties.put("keyStorePass", "flight");

        URI address = new URI("jdbc",
                FlightTestUtils.getUsername1()+ ":" + FlightTestUtils.getPassword1(),
                FlightTestUtils.getLocalhost(), this.tlsServer.getPort(),
                null, null, null);

        UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(
                FlightTestUtils.getUsername1(), FlightTestUtils.getPassword1());

        ArrowFlightClient client = ArrowFlightClient.getEncryptedClient(FlightTestUtils.getAllocator(),
                address, credentials, properties.getProperty("keyStorePath"),
                properties.getProperty("keyStorePass"));

        assertNotNull(client);
    }


    /**
     * Check if an encrypted connection can be established successfully when
     * the provided valid credentials and a valid Keystore.
     *
     * @throws SQLException on error.
     */
    @Test
    public void connectTls() throws  SQLException {
        Properties properties = new Properties();

        properties.put("user", FlightTestUtils.getUsername1());
        properties.put("pass", FlightTestUtils.getPassword1());
        properties.put("useTls", "true");
        properties.put("keyStorePath", "keyStore.jks");
        properties.put("keyStorePass", "flight");

        Connection connection = DriverManager.getConnection(serverUrl, properties);

        assertFalse(connection.isClosed());
    }
}

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

package org.apache.arrow.driver.jdbc.client;

import java.io.IOException;
import java.net.URI;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.client.ClientCookieMiddleware;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.flight.sql.util.TableRef;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.avatica.Meta.StatementType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link FlightSqlClient} handler.
 */
public final class ArrowFlightSqlClientHandler implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArrowFlightSqlClientHandler.class);

  private final FlightSqlClient sqlClient;
  private final Set<CallOption> options = new HashSet<>();
  private final Builder builder;

  ArrowFlightSqlClientHandler(final FlightSqlClient sqlClient,
                              final Builder builder,
                              final Collection<CallOption> credentialOptions) {
    this.options.addAll(builder.options);
    this.options.addAll(credentialOptions);
    this.sqlClient = Preconditions.checkNotNull(sqlClient);
    this.builder = builder;
  }

  /**
   * Creates a new {@link ArrowFlightSqlClientHandler} from the provided {@code client} and {@code options}.
   *
   * @param client  the {@link FlightClient} to manage under a {@link FlightSqlClient} wrapper.
   * @param options the {@link CallOption}s to persist in between subsequent client calls.
   * @return a new {@link ArrowFlightSqlClientHandler}.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final FlightClient client,
                                                             final Builder builder,
                                                             final Collection<CallOption> options) {
    return new ArrowFlightSqlClientHandler(new FlightSqlClient(client), builder, options);
  }

  /**
   * Gets the {@link #options} for the subsequent calls from this handler.
   *
   * @return the {@link CallOption}s.
   */
  private CallOption[] getOptions() {
    return options.toArray(new CallOption[0]);
  }

  /**
   * Makes an RPC "getStream" request based on the provided {@link FlightInfo}
   * object. Retrieves the result of the query previously prepared with "getInfo."
   *
   * @param flightInfo The {@link FlightInfo} instance from which to fetch results.
   * @return a {@code FlightStream} of results.
   */
  public List<CloseableEndpointStreamPair> getStreams(final FlightInfo flightInfo) throws SQLException {
    final ArrayList<CloseableEndpointStreamPair> endpoints =
        new ArrayList<>(flightInfo.getEndpoints().size());

    try {
      for (FlightEndpoint endpoint : flightInfo.getEndpoints()) {
        if (endpoint.getLocations().isEmpty()) {
          // Create a stream using the current client only and do not close the client at the end.
          endpoints.add(new CloseableEndpointStreamPair(
              sqlClient.getStream(endpoint.getTicket(), getOptions()), null));
        } else {
          // Clone the builder and then set the new endpoint on it.
          // GH-38573: This code currently only tries the first Location and treats a failure as fatal.
          // This should be changed to try other Locations that are available.
          
          // GH-38574: Currently a new FlightClient will be made for each partition that returns a non-empty Location
          // then disposed of. It may be better to cache clients because a server may report the same Locations.
          // It would also be good to identify when the reported location is the same as the original connection's
          // Location and skip creating a FlightClient in that scenario.
          final URI endpointUri = endpoint.getLocations().get(0).getUri();
          final Builder builderForEndpoint = new Builder(ArrowFlightSqlClientHandler.this.builder)
              .withHost(endpointUri.getHost())
              .withPort(endpointUri.getPort())
              .withEncryption(endpointUri.getScheme().equals(LocationSchemes.GRPC_TLS));

          final ArrowFlightSqlClientHandler endpointHandler = builderForEndpoint.build();
          try {
            endpoints.add(new CloseableEndpointStreamPair(
                endpointHandler.sqlClient.getStream(endpoint.getTicket(),
                    endpointHandler.getOptions()), endpointHandler.sqlClient));
          } catch (Exception ex) {
            AutoCloseables.close(endpointHandler);
            throw ex;
          }
        }
      }
    } catch (Exception outerException) {
      try {
        AutoCloseables.close(endpoints);
      } catch (Exception innerEx) {
        outerException.addSuppressed(innerEx);
      }

      if (outerException instanceof SQLException) {
        throw (SQLException) outerException;
      }
      throw new SQLException(outerException);
    }
    return endpoints;
  }

  /**
   * Makes an RPC "getInfo" request based on the provided {@code query}
   * object.
   *
   * @param query The query.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getInfo(final String query) {
    return sqlClient.execute(query, getOptions());
  }

  @Override
  public void close() throws SQLException {
    try {
      AutoCloseables.close(sqlClient);
    } catch (final Exception e) {
      throw new SQLException("Failed to clean up client resources.", e);
    }
  }

  /**
   * A prepared statement handler.
   */
  public interface PreparedStatement extends AutoCloseable {
    /**
     * Executes this {@link PreparedStatement}.
     *
     * @return the {@link FlightInfo} representing the outcome of this query execution.
     * @throws SQLException on error.
     */
    FlightInfo executeQuery() throws SQLException;

    /**
     * Executes a {@link StatementType#UPDATE} query.
     *
     * @return the number of rows affected.
     */
    long executeUpdate();

    /**
     * Gets the {@link StatementType} of this {@link PreparedStatement}.
     *
     * @return the Statement Type.
     */
    StatementType getType();

    /**
     * Gets the {@link Schema} of this {@link PreparedStatement}.
     *
     * @return {@link Schema}.
     */
    Schema getDataSetSchema();

    /**
     * Gets the {@link Schema} of the parameters for this {@link PreparedStatement}.
     *
     * @return {@link Schema}.
     */
    Schema getParameterSchema();

    void setParameters(VectorSchemaRoot parameters);

    @Override
    void close();
  }

  /**
   * Creates a new {@link PreparedStatement} for the given {@code query}.
   *
   * @param query the SQL query.
   * @return a new prepared statement.
   */
  public PreparedStatement prepare(final String query) {
    final FlightSqlClient.PreparedStatement preparedStatement =
        sqlClient.prepare(query, getOptions());
    return new PreparedStatement() {
      @Override
      public FlightInfo executeQuery() throws SQLException {
        return preparedStatement.execute(getOptions());
      }

      @Override
      public long executeUpdate() {
        return preparedStatement.executeUpdate(getOptions());
      }

      @Override
      public StatementType getType() {
        final Schema schema = preparedStatement.getResultSetSchema();
        return schema.getFields().isEmpty() ? StatementType.UPDATE : StatementType.SELECT;
      }

      @Override
      public Schema getDataSetSchema() {
        return preparedStatement.getResultSetSchema();
      }

      @Override
      public Schema getParameterSchema() {
        return preparedStatement.getParameterSchema();
      }

      @Override
      public void setParameters(VectorSchemaRoot parameters) {
        preparedStatement.setParameters(parameters);
      }

      @Override
      public void close() {
        try {
          preparedStatement.close(getOptions());
        } catch (FlightRuntimeException fre) {
          // ARROW-17785: suppress exceptions caused by flaky gRPC layer
          if (fre.status().code().equals(FlightStatusCode.UNAVAILABLE) ||
              (fre.status().code().equals(FlightStatusCode.INTERNAL) &&
                  fre.getMessage().contains("Connection closed after GOAWAY"))) {
            LOGGER.warn("Supressed error closing PreparedStatement", fre);
            return;
          }
          throw fre;
        }
      }
    };
  }

  /**
   * Makes an RPC "getCatalogs" request.
   *
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getCatalogs() {
    return sqlClient.getCatalogs(getOptions());
  }

  /**
   * Makes an RPC "getImportedKeys" request based on the provided info.
   *
   * @param catalog The catalog name. Must match the catalog name as it is stored in the database.
   *                Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                narrow the search.
   * @param schema  The schema name. Must match the schema name as it is stored in the database.
   *                "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                the search.
   * @param table   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getImportedKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getImportedKeys(TableRef.of(catalog, schema, table), getOptions());
  }

  /**
   * Makes an RPC "getExportedKeys" request based on the provided info.
   *
   * @param catalog The catalog name. Must match the catalog name as it is stored in the database.
   *                Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                narrow the search.
   * @param schema  The schema name. Must match the schema name as it is stored in the database.
   *                "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                the search.
   * @param table   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getExportedKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getExportedKeys(TableRef.of(catalog, schema, table), getOptions());
  }

  /**
   * Makes an RPC "getSchemas" request based on the provided info.
   *
   * @param catalog       The catalog name. Must match the catalog name as it is stored in the database.
   *                      Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                      narrow the search.
   * @param schemaPattern The schema name pattern. Must match the schema name as it is stored in the database.
   *                      Null means that schema name should not be used to narrow down the search.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getSchemas(final String catalog, final String schemaPattern) {
    return sqlClient.getSchemas(catalog, schemaPattern, getOptions());
  }

  /**
   * Makes an RPC "getTableTypes" request.
   *
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getTableTypes() {
    return sqlClient.getTableTypes(getOptions());
  }

  /**
   * Makes an RPC "getTables" request based on the provided info.
   *
   * @param catalog          The catalog name. Must match the catalog name as it is stored in the database.
   *                         Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                         narrow the search.
   * @param schemaPattern    The schema name pattern. Must match the schema name as it is stored in the database.
   *                         "" retrieves those without a schema. Null means that the schema name should not be used to
   *                         narrow the search.
   * @param tableNamePattern The table name pattern. Must match the table name as it is stored in the database.
   * @param types            The list of table types, which must be from the list of table types to include.
   *                         Null returns all types.
   * @param includeSchema    Whether to include schema.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getTables(final String catalog, final String schemaPattern,
                              final String tableNamePattern,
                              final List<String> types, final boolean includeSchema) {

    return sqlClient.getTables(catalog, schemaPattern, tableNamePattern, types, includeSchema,
        getOptions());
  }

  /**
   * Gets SQL info.
   *
   * @return the SQL info.
   */
  public FlightInfo getSqlInfo(SqlInfo... info) {
    return sqlClient.getSqlInfo(info, getOptions());
  }

  /**
   * Makes an RPC "getPrimaryKeys" request based on the provided info.
   *
   * @param catalog The catalog name; must match the catalog name as it is stored in the database.
   *                "" retrieves those without a catalog.
   *                Null means that the catalog name should not be used to narrow the search.
   * @param schema  The schema name; must match the schema name as it is stored in the database.
   *                "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                the search.
   * @param table   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getPrimaryKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getPrimaryKeys(TableRef.of(catalog, schema, table), getOptions());
  }

  /**
   * Makes an RPC "getCrossReference" request based on the provided info.
   *
   * @param pkCatalog The catalog name. Must match the catalog name as it is stored in the database.
   *                  Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                  narrow the search.
   * @param pkSchema  The schema name. Must match the schema name as it is stored in the database.
   *                  "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                  the search.
   * @param pkTable   The table name. Must match the table name as it is stored in the database.
   * @param fkCatalog The catalog name. Must match the catalog name as it is stored in the database.
   *                  Retrieves those without a catalog. Null means that the catalog name should not be used to
   *                  narrow the search.
   * @param fkSchema  The schema name. Must match the schema name as it is stored in the database.
   *                  "" retrieves those without a schema. Null means that the schema name should not be used to narrow
   *                  the search.
   * @param fkTable   The table name. Must match the table name as it is stored in the database.
   * @return a {@code FlightStream} of results.
   */
  public FlightInfo getCrossReference(String pkCatalog, String pkSchema, String pkTable,
                                      String fkCatalog, String fkSchema, String fkTable) {
    return sqlClient.getCrossReference(TableRef.of(pkCatalog, pkSchema, pkTable),
        TableRef.of(fkCatalog, fkSchema, fkTable),
        getOptions());
  }

  /**
   * Builder for {@link ArrowFlightSqlClientHandler}.
   */
  public static final class Builder {
    private final Set<FlightClientMiddleware.Factory> middlewareFactories = new HashSet<>();
    private final Set<CallOption> options = new HashSet<>();
    private String host;
    private int port;

    @VisibleForTesting
    String username;

    @VisibleForTesting
    String password;

    @VisibleForTesting
    String trustStorePath;

    @VisibleForTesting
    String trustStorePassword;

    @VisibleForTesting
    String token;

    @VisibleForTesting
    boolean useEncryption = true;

    @VisibleForTesting
    boolean disableCertificateVerification;

    @VisibleForTesting
    boolean useSystemTrustStore = true;

    @VisibleForTesting
    String tlsRootCertificatesPath;

    @VisibleForTesting
    String clientCertificatePath;

    @VisibleForTesting
    String clientKeyPath;

    @VisibleForTesting
    private BufferAllocator allocator;

    @VisibleForTesting
    boolean retainCookies = true;

    @VisibleForTesting
    boolean retainAuth = true;

    // These two middleware are for internal use within build() and should not be exposed by builder APIs.
    // Note that these middleware may not necessarily be registered.
    @VisibleForTesting
    ClientIncomingAuthHeaderMiddleware.Factory authFactory
        = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());

    @VisibleForTesting
    ClientCookieMiddleware.Factory cookieFactory = new ClientCookieMiddleware.Factory();

    public Builder() {
    }

    /**
     * Copies the builder.
     *
     * @param original The builder to base this copy off of.
     */
    @VisibleForTesting
    Builder(Builder original) {
      this.middlewareFactories.addAll(original.middlewareFactories);
      this.options.addAll(original.options);
      this.host = original.host;
      this.port = original.port;
      this.username = original.username;
      this.password = original.password;
      this.trustStorePath = original.trustStorePath;
      this.trustStorePassword = original.trustStorePassword;
      this.token = original.token;
      this.useEncryption = original.useEncryption;
      this.disableCertificateVerification = original.disableCertificateVerification;
      this.useSystemTrustStore = original.useSystemTrustStore;
      this.tlsRootCertificatesPath = original.tlsRootCertificatesPath;
      this.clientCertificatePath = original.clientCertificatePath;
      this.clientKeyPath = original.clientKeyPath;
      this.allocator = original.allocator;

      if (original.retainCookies) {
        this.cookieFactory = original.cookieFactory;
      }

      if (original.retainAuth) {
        this.authFactory = original.authFactory;
      }
    }

    /**
     * Sets the host for this handler.
     *
     * @param host the host.
     * @return this instance.
     */
    public Builder withHost(final String host) {
      this.host = host;
      return this;
    }

    /**
     * Sets the port for this handler.
     *
     * @param port the port.
     * @return this instance.
     */
    public Builder withPort(final int port) {
      this.port = port;
      return this;
    }

    /**
     * Sets the username for this handler.
     *
     * @param username the username.
     * @return this instance.
     */
    public Builder withUsername(final String username) {
      this.username = username;
      return this;
    }

    /**
     * Sets the password for this handler.
     *
     * @param password the password.
     * @return this instance.
     */
    public Builder withPassword(final String password) {
      this.password = password;
      return this;
    }

    /**
     * Sets the KeyStore path for this handler.
     *
     * @param trustStorePath the KeyStore path.
     * @return this instance.
     */
    public Builder withTrustStorePath(final String trustStorePath) {
      this.trustStorePath = trustStorePath;
      return this;
    }

    /**
     * Sets the KeyStore password for this handler.
     *
     * @param trustStorePassword the KeyStore password.
     * @return this instance.
     */
    public Builder withTrustStorePassword(final String trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
    }

    /**
     * Sets whether to use TLS encryption in this handler.
     *
     * @param useEncryption whether to use TLS encryption.
     * @return this instance.
     */
    public Builder withEncryption(final boolean useEncryption) {
      this.useEncryption = useEncryption;
      return this;
    }

    /**
     * Sets whether to disable the certificate verification in this handler.
     *
     * @param disableCertificateVerification whether to disable certificate verification.
     * @return this instance.
     */
    public Builder withDisableCertificateVerification(final boolean disableCertificateVerification) {
      this.disableCertificateVerification = disableCertificateVerification;
      return this;
    }

    /**
     * Sets whether to use the certificates from the operating system.
     *
     * @param useSystemTrustStore whether to use the system operating certificates.
     * @return this instance.
     */
    public Builder withSystemTrustStore(final boolean useSystemTrustStore) {
      this.useSystemTrustStore = useSystemTrustStore;
      return this;
    }

    /**
     * Sets the TLS root certificate path as an alternative to using the System
     * or other Trust Store.  The path must contain a valid PEM file.
     *
     * @param tlsRootCertificatesPath the TLS root certificate path (if TLS is required).
     * @return this instance.
     */
    public Builder withTlsRootCertificates(final String tlsRootCertificatesPath) {
      this.tlsRootCertificatesPath = tlsRootCertificatesPath;
      return this;
    }

    /**
     * Sets the mTLS client certificate path (if mTLS is required).
     *
     * @param clientCertificatePath the mTLS client certificate path (if mTLS is required).
     * @return this instance.
     */
    public Builder withClientCertificate(final String clientCertificatePath) {
      this.clientCertificatePath = clientCertificatePath;
      return this;
    }

    /**
     * Sets the mTLS client certificate private key path (if mTLS is required).
     *
     * @param clientKeyPath the mTLS client certificate private key path (if mTLS is required).
     * @return this instance.
     */
    public Builder withClientKey(final String clientKeyPath) {
      this.clientKeyPath = clientKeyPath;
      return this;
    }
    
    /**
     * Sets the token used in the token authentication.
     *
     * @param token the token value.
     * @return      this builder instance.
     */
    public Builder withToken(final String token) {
      this.token = token;
      return this;
    }

    /**
     * Sets the {@link BufferAllocator} to use in this handler.
     *
     * @param allocator the allocator.
     * @return this instance.
     */
    public Builder withBufferAllocator(final BufferAllocator allocator) {
      this.allocator = allocator
          .newChildAllocator("ArrowFlightSqlClientHandler", 0, allocator.getLimit());
      return this;
    }

    /**
     * Indicates if cookies should be re-used by connections spawned for getStreams() calls.
     * @param retainCookies The flag indicating if cookies should be re-used.
     * @return      this builder instance.
     */
    public Builder withRetainCookies(boolean retainCookies) {
      this.retainCookies = retainCookies;
      return this;
    }

    /**
     * Indicates if bearer tokens negotiated should be re-used by connections
     * spawned for getStreams() calls.
     *
     * @param retainAuth The flag indicating if auth tokens should be re-used.
     * @return      this builder instance.
     */
    public Builder withRetainAuth(boolean retainAuth) {
      this.retainAuth = retainAuth;
      return this;
    }

    /**
     * Adds the provided {@code factories} to the list of {@link #middlewareFactories} of this handler.
     *
     * @param factories the factories to add.
     * @return this instance.
     */
    public Builder withMiddlewareFactories(final FlightClientMiddleware.Factory... factories) {
      return withMiddlewareFactories(Arrays.asList(factories));
    }

    /**
     * Adds the provided {@code factories} to the list of {@link #middlewareFactories} of this handler.
     *
     * @param factories the factories to add.
     * @return this instance.
     */
    public Builder withMiddlewareFactories(
        final Collection<FlightClientMiddleware.Factory> factories) {
      this.middlewareFactories.addAll(factories);
      return this;
    }

    /**
     * Adds the provided {@link CallOption}s to this handler.
     *
     * @param options the options
     * @return this instance.
     */
    public Builder withCallOptions(final CallOption... options) {
      return withCallOptions(Arrays.asList(options));
    }

    /**
     * Adds the provided {@link CallOption}s to this handler.
     *
     * @param options the options
     * @return this instance.
     */
    public Builder withCallOptions(final Collection<CallOption> options) {
      this.options.addAll(options);
      return this;
    }

    /**
     * Builds a new {@link ArrowFlightSqlClientHandler} from the provided fields.
     *
     * @return a new client handler.
     * @throws SQLException on error.
     */
    public ArrowFlightSqlClientHandler build() throws SQLException {
      // Copy middleware so that the build method doesn't change the state of the builder fields itself.
      Set<FlightClientMiddleware.Factory> buildTimeMiddlewareFactories = new HashSet<>(this.middlewareFactories);
      FlightClient client = null;
      boolean isUsingUserPasswordAuth = username != null && token == null;

      try {
        // Token should take priority since some apps pass in a username/password even when a token is provided
        if (isUsingUserPasswordAuth) {
          buildTimeMiddlewareFactories.add(authFactory);
        }
        final FlightClient.Builder clientBuilder = FlightClient.builder().allocator(allocator);

        buildTimeMiddlewareFactories.add(new ClientCookieMiddleware.Factory());
        buildTimeMiddlewareFactories.forEach(clientBuilder::intercept);
        Location location;
        if (useEncryption) {
          location = Location.forGrpcTls(host, port);
          clientBuilder.useTls();
        } else {
          location = Location.forGrpcInsecure(host, port);
        }
        clientBuilder.location(location);

        if (useEncryption) {
          if (disableCertificateVerification) {
            clientBuilder.verifyServer(false);
          } else {
            if (tlsRootCertificatesPath != null) {
              clientBuilder.trustedCertificates(
                      ClientAuthenticationUtils.getTlsRootCertificatesStream(tlsRootCertificatesPath));
            } else if (useSystemTrustStore) {
              clientBuilder.trustedCertificates(
                  ClientAuthenticationUtils.getCertificateInputStreamFromSystem(trustStorePassword));
            } else if (trustStorePath != null) {
              clientBuilder.trustedCertificates(
                  ClientAuthenticationUtils.getCertificateStream(trustStorePath, trustStorePassword));
            }
          }

          if (clientCertificatePath != null && clientKeyPath != null) {
            clientBuilder.clientCertificate(
                ClientAuthenticationUtils.getClientCertificateStream(clientCertificatePath),
                ClientAuthenticationUtils.getClientKeyStream(clientKeyPath));
          }
        }

        client = clientBuilder.build();
        final ArrayList<CallOption> credentialOptions = new ArrayList<>();
        if (isUsingUserPasswordAuth) {
          // If the authFactory has already been used for a handshake, use the existing token.
          // This can occur if the authFactory is being re-used for a new connection spawned for getStream().
          if (authFactory.getCredentialCallOption() != null) {
            credentialOptions.add(authFactory.getCredentialCallOption());
          } else {
            // Otherwise do the handshake and get the token if possible.
            credentialOptions.add(
                ClientAuthenticationUtils.getAuthenticate(
                    client, username, password, authFactory, options.toArray(new CallOption[0])));
          }
        } else if (token != null) {
          credentialOptions.add(
              ClientAuthenticationUtils.getAuthenticate(
                  client, new CredentialCallOption(new BearerCredentialWriter(token)), options.toArray(
                          new CallOption[0])));
        }
        return ArrowFlightSqlClientHandler.createNewHandler(client, this, credentialOptions);

      } catch (final IllegalArgumentException | GeneralSecurityException | IOException | FlightRuntimeException e) {
        final SQLException originalException = new SQLException(e);
        if (client != null) {
          try {
            client.close();
          } catch (final InterruptedException interruptedException) {
            originalException.addSuppressed(interruptedException);
          }
        }
        throw originalException;
      }
    }
  }
}

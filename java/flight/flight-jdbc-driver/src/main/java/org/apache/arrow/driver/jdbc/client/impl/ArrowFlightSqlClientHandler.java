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

package org.apache.arrow.driver.jdbc.client.impl;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.driver.jdbc.client.utils.ClientAuthenticationUtils;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.ClientBearerHeaderHandler;
import org.apache.arrow.flight.auth2.ClientIncomingAuthHeaderMiddleware;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;

/**
 * A {@link FlightClientHandler} for a {@link FlightSqlClient}.
 */
public final class ArrowFlightSqlClientHandler extends ArrowFlightClientHandler {

  private final FlightSqlClient sqlClient;

  ArrowFlightSqlClientHandler(final FlightSqlClient sqlClient,
                              final Collection<CallOption> options) {
    super(options);
    this.sqlClient = Preconditions.checkNotNull(sqlClient);
  }

  /**
   * Creates a new {@link ArrowFlightSqlClientHandler} from the provided {@code client} and {@code options}.
   *
   * @param client  the {@link FlightClient} to manage under a {@link FlightSqlClient} wrapper.
   * @param options the {@link CallOption}s to persist in between subsequent client calls.
   * @return a new {@link FlightClientHandler}.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final FlightClient client,
                                                             final Collection<CallOption> options) {
    return new ArrowFlightSqlClientHandler(new FlightSqlClient(client), options);
  }

  @Override
  public List<FlightStream> getStreams(final FlightInfo flightInfo) {
    return flightInfo.getEndpoints().stream()
        .map(FlightEndpoint::getTicket)
        .map(ticket -> sqlClient.getStream(ticket, getOptions()))
        .collect(Collectors.toList());
  }

  @Override
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

  @Override
  public PreparedStatement prepare(final String query) {
    final FlightSqlClient.PreparedStatement preparedStatement = sqlClient.prepare(query, getOptions());
    return new PreparedStatement() {
      @Override
      public FlightInfo executeQuery() throws SQLException {
        return preparedStatement.execute(getOptions());
      }

      @Override
      public void close() {
        preparedStatement.close(getOptions());
      }
    };
  }

  @Override
  public FlightInfo getCatalogs() {
    return sqlClient.getCatalogs(getOptions());
  }

  @Override
  public FlightInfo getImportedKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getImportedKeys(catalog, schema, table, getOptions());
  }

  @Override
  public FlightInfo getExportedKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getExportedKeys(catalog, schema, table, getOptions());
  }

  @Override
  public FlightInfo getSchemas(final String catalog, final String schemaPattern) {
    return sqlClient.getSchemas(catalog, schemaPattern, getOptions());
  }

  @Override
  public FlightInfo getTableTypes() {
    return sqlClient.getTableTypes(getOptions());
  }

  @Override
  public FlightInfo getTables(final String catalog, final String schemaPattern, final String tableNamePattern,
                              final String[] types, final boolean includeSchema) {

    return sqlClient.getTables(catalog, schemaPattern,
        tableNamePattern, types != null ? Arrays.asList(types) : null, includeSchema,
        getOptions());
  }

  @Override
  public FlightInfo getSqlInfo(SqlInfo... info) {
    return sqlClient.getSqlInfo(info, getOptions());
  }

  @Override
  public FlightInfo getPrimaryKeys(final String catalog, final String schema, final String table) {
    return sqlClient.getPrimaryKeys(catalog, schema, table, getOptions());
  }

  /**
   * Builder for {@link ArrowFlightSqlClientHandler}.
   */
  public static final class Builder {
    private String host;
    private int port;
    private String username;
    private String password;
    private String keyStorePath;
    private String keyStorePassword;
    private boolean useTls;
    private BufferAllocator allocator;
    private final Set<FlightClientMiddleware.Factory> middlewareFactories = new HashSet<>();
    private final Set<CallOption> options = new HashSet<>();

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
     * @param keyStorePath the KeyStore path.
     * @return this instance.
     */
    public Builder withKeyStorePath(final String keyStorePath) {
      this.keyStorePath = keyStorePath;
      return this;
    }

    /**
     * Sets the KeyStore password for this handler.
     *
     * @param keyStorePassword the KeyStore password.
     * @return this instance.
     */
    public Builder withKeyStorePassword(final String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    /**
     * Sets whether to use TLS encryption in this handler.
     *
     * @param useTls whether to use TLS encryption.
     * @return this instance.
     */
    public Builder withTlsEncryption(final boolean useTls) {
      this.useTls = useTls;
      return this;
    }

    /**
     * Sets the {@link BufferAllocator} to use in this handler.
     *
     * @param allocator the allocator.
     * @return this instance.
     */
    public Builder withBufferAllocator(final BufferAllocator allocator) {
      this.allocator = allocator;
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
    public Builder withMiddlewareFactories(final Collection<FlightClientMiddleware.Factory> factories) {
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
      try {
        ClientIncomingAuthHeaderMiddleware.Factory authFactory = null;
        if (username != null) {
          authFactory = new ClientIncomingAuthHeaderMiddleware.Factory(new ClientBearerHeaderHandler());
          withMiddlewareFactories(authFactory);
        }
        final FlightClient.Builder clientBuilder = FlightClient.builder().allocator(allocator);
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
          clientBuilder.trustedCertificates(
              ClientAuthenticationUtils.getCertificateStream(keyStorePath, keyStorePassword));
        }
        final FlightClient client = clientBuilder.build();
        if (authFactory != null) {
          options.add(ClientAuthenticationUtils.getAuthenticate(client, username, password, authFactory));
        }
        return ArrowFlightSqlClientHandler.createNewHandler(client, options);
      } catch (final IllegalArgumentException | GeneralSecurityException | IOException e) {
        throw new SQLException(e);
      }
    }
  }
}

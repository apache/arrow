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

package org.apache.arrow.driver.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

/**
 * JDBC driver for querying data from an Apache Arrow Flight server.
 */
public class ArrowFlightJdbcDriver extends UnregisteredDriver {

  private static final String CONNECT_STRING_PREFIX = "jdbc:arrow-flight://";

  static {
    (new ArrowFlightJdbcDriver()).register();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {

    Properties clonedProperties = (Properties) info.clone();

    try {
      String[] args = getUrlsArgs(Preconditions.checkNotNull(url));

      addToProperties(clonedProperties, args);

      return new ArrowFlightConnection(this, factory, url, clonedProperties);
    } catch (AssertionError | FlightRuntimeException e) {
      throw new SQLException("Failed to connect.", e);
    }
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return Version.CURRENT.getDriverVersion();
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new ArrowFlightMetaImpl(connection);
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return Preconditions.checkNotNull(url).startsWith(CONNECT_STRING_PREFIX);
  }

  /**
   * Parses the provided url based on the format this driver accepts, retrieving
   * arguments after the {@link #CONNECT_STRING_PREFIX}.
   *
   * @param url
   *          The url to parse.
   * @return the parsed arguments.
   * @throws SQLException
   *           If an error occurs while trying to parse the URL.
   */
  private String[] getUrlsArgs(String url) throws SQLException {
    // URL must ALWAYS start with "jdbc:arrow-flight://"
    assert acceptsURL(url);

    /*
     * Granted the URL format will always be
     * "jdbc:arrow-flight://<host>:<port>," it should be safe to
     * split the URL arguments "host," "port" by the colon in between.
     */
    return url.substring(getConnectStringPrefix().length()).split(":");
  }

  private static void addToProperties(Properties info, String... args) {
    String host = (String) args[0];
    int port = Integer.parseInt(args[1]);

    Preconditions.checkNotNull(info).put("host", host);
    info.put("port", port);
  }

  /**
   * Enum representation of this driver's version.
   */
  public enum Version {
    // TODO Double-check this.
    CURRENT(new DriverVersion("Arrow Flight JDBC Driver", "0.0.1-SNAPSHOT",
        "Arrow Flight", "0.0.1-SNAPSHOT", true, 0, 1, 0, 1));

    private final DriverVersion driverVersion;

    private Version(DriverVersion driverVersion) {
      this.driverVersion = Preconditions.checkNotNull(driverVersion);
    }

    public final DriverVersion getDriverVersion() {
      return driverVersion;
    }
  }
}

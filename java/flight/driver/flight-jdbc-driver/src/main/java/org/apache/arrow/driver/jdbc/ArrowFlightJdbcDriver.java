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

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
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

  public ArrowFlightJdbcDriver() {
  }

  static {
    (new ArrowFlightJdbcDriver()).register();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {

    ArrowFlightConnection connection = null;

    Create: {
      if (!this.acceptsURL(url)) {
        break Create;
      }

      String[] args = getUrlsArgs(url);
      info.put("host", args[0]);
      info.put("port", args[1]);

      try {
        connection = new ArrowFlightConnection(this, factory, url, info);
      } catch (KeyStoreException | NoSuchAlgorithmException
          | CertificateException | IOException | NumberFormatException
          | URISyntaxException e) {
        e.printStackTrace();
      }
    }

    return connection;
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

  /**
   * Parses the provided url based on the format this driver accepts, retrieving
   * arguments after the {@link #CONNECT_STRING_PREFIX}.
   *
   * @param url
   *          The url to parse.
   * @return the parsed arguments.
   */
  private final String[] getUrlsArgs(String url) {
    assert Preconditions.checkNotNull(url).startsWith(getConnectStringPrefix());
    return url.substring(getConnectStringPrefix().length()).split(":");
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

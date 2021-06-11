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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.arrow.driver.jdbc.utils.DefaultProperty;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import com.google.common.base.Strings;

/**
 * JDBC driver for querying data from an Apache Arrow Flight server.
 */
public class ArrowFlightJdbcDriver extends UnregisteredDriver {

  private static final String CONNECT_STRING_PREFIX = "jdbc:arrow-flight://";
  private static final Pattern urlRegExPattern = Pattern.compile("^(" +
      CONNECT_STRING_PREFIX + ")" +
      "(\\w+):([\\d]+)\\/*\\?*([[\\w]+=[\\w]+&?]*)?");
      
  private static DriverVersion version;

  static {
    (new ArrowFlightJdbcDriver()).register();
  }

  @Override
  public Connection connect(final String url, final Properties info)
      throws SQLException {

    final Properties clonedProperties = (Properties) info.clone();

    try {
      final Map<String, String> args = getUrlsArgs(
          Preconditions.checkNotNull(url));

      clonedProperties.putAll(args);

      return new ArrowFlightConnection(this, factory, url, clonedProperties);
    } catch (AssertionError | FlightRuntimeException e) {
      throw new SQLException("Failed to connect.", e);
    }
  }

  @Override
  protected String getFactoryClassName(final JdbcVersion jdbcVersion) {
    return ArrowFlightJdbcFactory.class.getName();
  }

  @Override
  protected DriverVersion createDriverVersion() {

    CreateVersionIfNull: {

      if (version != null) {
        break CreateVersionIfNull;
      }

      try (Reader reader =
          new BufferedReader(new InputStreamReader(
              new FileInputStream("target/flight.properties"), "UTF-8"))) {
        Properties properties = new Properties();
        properties.load(reader);

        String parentName = properties.getProperty(
            "org.apache.arrow.flight.name");
        String parentVersion = properties.getProperty(
            "org.apache.arrow.flight.version");
        String[] pVersion = parentVersion.split("\\.");

        int parentMajorVersion = Integer.parseInt(pVersion[0]);
        int parentMinorVersion = Integer.parseInt(pVersion[1]);

        String childName = properties.getProperty(
            "org.apache.arrow.flight.jdbc-driver.name");
        String childVersion = properties.getProperty(
            "org.apache.arrow.flight.jdbc-driver.version");
        String[] cVersion = childVersion.split("\\.");

        int childMajorVersion = Integer.parseInt(cVersion[0]);
        int childMinorVersion = Integer.parseInt(cVersion[1]);

        version = new DriverVersion(childName, childVersion, parentName,
            parentVersion, true, childMajorVersion, childMinorVersion,
            parentMajorVersion, parentMinorVersion);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load driver version.", e);
      }
    }

    return version;
  }

  @Override
  public Meta createMeta(final AvaticaConnection connection) {
    return new ArrowFlightMetaImpl(connection);
  }

  @Override
  protected String getConnectStringPrefix() {
    return CONNECT_STRING_PREFIX;
  }

  @Override
  public boolean acceptsURL(final String url) throws SQLException {
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
  private Map<String, String> getUrlsArgs(final String url)
      throws SQLException {

    /*
     * URL must ALWAYS follow the pattern:
     * "jdbc:arrow-flight://<host>:<port>[/?param1=value1&param2=value2&(...)]."
     */
    final Matcher matcher = urlRegExPattern.matcher(url);
    
    if (!matcher.matches()) {
      throw new SQLException("Malformed/invalid URL!");
    }

    final Map<String, String> resultMap = new HashMap<>();

    // Group 1 contains the prefix -- start from 2.
    resultMap.put(DefaultProperty.HOST.toString(), matcher.group(2));
    resultMap.put(DefaultProperty.PORT.toString(), matcher.group(3));

    // Group 4 contains all optional parameters, if provided -- must check.
    final String extraParams = matcher.group(4);

    if (!Strings.isNullOrEmpty(extraParams)) {
      for (final String params : extraParams.split("&")) {
        final String[] keyValuePair = params.split("=");
        if (keyValuePair.length != 2) {
          throw new SQLException(
              "URL parameters must be provided in key-value pairs!");
        }
        resultMap.put(keyValuePair[0], keyValuePair[1]);
      }
    }

    return resultMap;
  }
}

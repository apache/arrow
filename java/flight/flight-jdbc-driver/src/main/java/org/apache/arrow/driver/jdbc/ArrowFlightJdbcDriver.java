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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.RegEx;

import org.apache.arrow.driver.jdbc.utils.DefaultProperty;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.maven.model.Model;
import org.apache.maven.model.Parent;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import com.google.common.base.Strings;

/**
 * JDBC driver for querying data from an Apache Arrow Flight server.
 */
public class ArrowFlightJdbcDriver extends UnregisteredDriver {

  private static final String CONNECT_STRING_PREFIX = "jdbc:arrow-flight://";
  private static DriverVersion version;

  static {
    (new ArrowFlightJdbcDriver()).register();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {

    Properties clonedProperties = (Properties) info.clone();

    try {
      Map<String, String> args = getUrlsArgs(Preconditions.checkNotNull(url));

      clonedProperties.putAll(args);

      return new ArrowFlightConnection(this, factory, url, clonedProperties);
    } catch (AssertionError | FlightRuntimeException e) {
      throw new SQLException("Failed to connect.", e);
    }
  }

  @Override
  protected String getFactoryClassName(JdbcVersion jdbcVersion) {
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
                new FileInputStream("pom.xml"), "UTF-8"))) {
  
        Model flightJdbcDriverPom = (new MavenXpp3Reader()).read(reader);
        Parent arrowFlightPom = flightJdbcDriverPom.getParent();
  
        String parentVersion = arrowFlightPom.getVersion();
        String childVersion = flightJdbcDriverPom.getVersion();
  
        int[] childVersionParts =
            Arrays.stream(parentVersion.split("\\.")).limit(2)
              .mapToInt(Integer::parseInt).toArray();
        
        int[] parentVersionParts =
            Arrays.stream(parentVersion.split("\\.")).limit(2)
              .mapToInt(Integer::parseInt).toArray();
  
        version = new DriverVersion(flightJdbcDriverPom.getName(), childVersion,
            arrowFlightPom.getId(), parentVersion, true, childVersionParts[0],
            childVersionParts[1], parentVersionParts[0], parentVersionParts[1]);
      } catch (IOException | XmlPullParserException e) {
        throw new RuntimeException("Failed to load driver version.", e);
      }
    
    }

    return version;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new ArrowFlightMetaImpl((ArrowFlightConnection) connection);
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
  private Map<String, String> getUrlsArgs(String url) throws SQLException {
    @RegEx
    final String regex =
        "^(" + getConnectStringPrefix() + ")" +
            "(\\w+):([\\d]+)\\/*\\?*([[\\w]*=[\\w]*&?]*)?";

    /*
     * URL must ALWAYS start follow pattern
     * "jdbc:arrow-flight://<host>:<port>[/?param1=value1&param2=value2&(...)]."
     */
    final Matcher matcher = Pattern.compile(regex).matcher(url);
    assert matcher.matches();

    final Map<String, String> resultMap = new HashMap<>();

    // Group 1 contains the prefix -- start from 2.
    resultMap.put(DefaultProperty.HOST.toString(), matcher.group(2));
    resultMap.put(DefaultProperty.PORT.toString(), matcher.group(3));

    // Group 4 contains all optional parameters, if provided -- must check.
    final String group4 = matcher.group(4);

    if (!Strings.isNullOrEmpty(group4)) {
      for (String params : group4.split("&")) {
        final String[] keyValuePair = params.split("=");
        resultMap.put(keyValuePair[0], keyValuePair[1]);
      }
    }

    return resultMap;
  }
}

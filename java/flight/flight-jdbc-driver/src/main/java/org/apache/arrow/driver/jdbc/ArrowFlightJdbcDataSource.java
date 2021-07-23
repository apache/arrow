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

import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;

/**
 * DataSource implementation for ArrowFlightJdbcDriver.
 */
public class ArrowFlightJdbcDataSource extends BasicDataSource {

  public static final String DRIVER_CLASS_NAME = "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver";

  /**
   * Instantiates a DataSource that connects to given URL using {@link ArrowFlightJdbcDriver}.
   */
  public ArrowFlightJdbcDataSource(String connectionUri, Properties properties) {
    super();

    loadDriver();
    this.setDriverClassName(DRIVER_CLASS_NAME);
    this.setUrl(connectionUri);

    properties.forEach((key, value) -> this.addConnectionProperty(((String) key), ((String) value)));
  }

  private static void loadDriver() {
    try {
      Class.forName(DRIVER_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}

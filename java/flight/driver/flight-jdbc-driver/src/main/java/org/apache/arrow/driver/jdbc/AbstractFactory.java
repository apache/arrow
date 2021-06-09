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

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;

/**
 * Partial implementation of {@link AvaticaFactory}
 * (factory for main JDBC objects) for Arrow Flight JDBC's driver.
 */
abstract class AbstractFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  public AbstractFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  public int getMajor() {
    return major;
  }

  public int getMinor() {
    return minor;
  }

  @Override
  public AvaticaConnection newConnection(UnregisteredDriver driver,
                                         AvaticaFactory factory,
                                         String url, Properties info) throws SQLException {
    return newConnection((ArrowFlightJdbcDriver) driver, (AbstractFactory) factory, url, info);
  }

  abstract ArrowFlightConnection newConnection(ArrowFlightJdbcDriver driver,
                                               AbstractFactory factory,
                                               String url,
                                               Properties info) throws SQLException;
}

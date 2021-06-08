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

package org.apache.arrow.driver.jdbc.test.utils;

import javax.annotation.Nullable;

import org.apache.arrow.util.Preconditions;

import com.google.common.base.Optional;

/**
 * Class for storing sample JDBC URLs. Used for testing.
 */
public enum UrlSample {
  CONFORMING("jdbc:arrow-flight://", "localhost", 32010,
      "sample"), UNSUPPORTED("jdbc:mysql://", "localhost", 3306,
          "sample-catalog");

  private final String prefix;
  private final String host;
  private final int port;
  private final Optional<String> catalog;

  private UrlSample(String prefix, String host, int port,
      @Nullable String catalog) {
    this.prefix = Preconditions.checkNotNull(prefix);
    this.host = Preconditions.checkNotNull(host);
    this.port = Preconditions.checkElementIndex(port, Integer.MAX_VALUE);
    this.catalog = Optional.of(catalog);
  }

  /**
   * Gets the URL prefix.
   *
   * @return the prefix.
   */
  public final String getPrefix() {
    return prefix;
  }

  /**
   * Gets the host name.
   *
   * @return the host.
   */
  public final String getHost() {
    return host;
  }

  /**
   * Gets the port number.
   *
   * @return the port.
   */
  public final int getPort() {
    return port;
  }

  /**
   * Gets the catalog name.
   *
   * @return the catalog.
   */
  public final Optional<String> getCatalog() {
    return catalog;
  }

  /**
   * Gets the full URL.
   *
   * @return the URL.
   */
  public final String getPath() {
    return getPrefix() + getHost() + ":" + getPort() +
        (getCatalog().isPresent() ? "/" + getCatalog() : "");
  }
}

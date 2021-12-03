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

package org.apache.arrow.driver.jdbc.utils;

import org.apache.arrow.util.Preconditions;

/**
 * Class for storing sample JDBC URLs. Used for testing.
 *
 * @see org.apache.arrow.driver.jdbc.utils.BaseProperty
 * @deprecated not updatable to match dinamic server allocation.
 */
@Deprecated
public enum UrlSample {
  CONFORMING("jdbc:arrow-flight://", "localhost", 32010),
  UNSUPPORTED("jdbc:mysql://", "localhost", 3306);

  private final String prefix;
  private final String host;
  private final int port;

  private UrlSample(String prefix, String host, int port) {
    this.prefix = Preconditions.checkNotNull(prefix);
    this.host = Preconditions.checkNotNull(host);
    this.port = Preconditions.checkElementIndex(port, Integer.MAX_VALUE);
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
   * @see BaseProperty#getEntry
   * @deprecated outdated.
   */
  @Deprecated
  public final String getHost() {
    return host;
  }

  /**
   * Gets the port number.
   *
   * @return the port.
   * @see BaseProperty#getEntry
   * @deprecated outdated.
   */
  @Deprecated
  public final int getPort() {
    return port;
  }

  /**
   * Gets the full URL.
   *
   * @return the URL.
   */
  public final String getPath() {
    return getPrefix() + getHost() + ":" + getPort();
  }
}

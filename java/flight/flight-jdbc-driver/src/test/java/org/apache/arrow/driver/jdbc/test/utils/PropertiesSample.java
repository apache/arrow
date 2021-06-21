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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import javax.annotation.Nullable;

import org.apache.arrow.driver.jdbc.test.FlightServerTestRule;
import org.apache.arrow.util.Preconditions;


/**
 * {@link Properties} wrapper used for testing. Uses sample values.
 * @deprecated not updatable to match dinamic server allocation.
 * @see org.apache.arrow.driver.jdbc.test.FlightServerTestRule
 */
@Deprecated
public enum PropertiesSample {
  CONFORMING(UrlSample.CONFORMING, "user", "flight", "password",
      "flight123"), UNSUPPORTED(UrlSample.UNSUPPORTED, "user", "", "password",
          "");

  private final Properties properties = new Properties();

  private PropertiesSample(UrlSample url, @Nullable Object... properties) {
    loadProperties(url, properties);
  }

  /**
   * Returns default properties.
   *
   * @deprecated not updatable to match dinamic server allocation.
   * @see FlightServerTestRule#getProperties
   */
  @Deprecated
  public final Properties getProperties() {
    return properties;
  }

  private void loadProperties(UrlSample url,
      @Nullable Object... properties) {

    this.properties.put("host", url.getHost());
    this.properties.put("port", url.getPort());

    if (properties == null) {
      return;
    }

    Preconditions.checkArgument(properties.length % 2 == 0,
        "Properties must be provided as key-value pairs");

    Iterator<Object> iterator = Arrays.asList(properties).iterator();

    while (iterator.hasNext()) {
      this.properties.put(iterator.next(), iterator.next());
    }
  }
}

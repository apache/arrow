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

import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.THREAD_POOL_SIZE;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USER;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USE_TLS;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.PropertiesUtils.copyReplace;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;

@RunWith(Parameterized.class)
public final class PropertiesUtilsTest {
  private final Properties properties = new Properties();
  @Parameter
  public Map<ArrowFlightConnectionProperty, Object> replacements;
  private final Properties expectedProperties = new Properties();

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Before
  public void setUp() {
    properties.put(HOST.camelName(), "localhost");
    properties.put(PORT.camelName(), 32210);
    properties.put(USER.camelName(), "username");
    properties.put(PASSWORD.camelName(), "password");
    properties.put(USE_TLS.camelName(), false);
    properties.put(THREAD_POOL_SIZE.camelName(), 4);
    expectedProperties.putAll(properties);
    replacements.forEach((k, v) -> expectedProperties.replace(k.camelName(), v));
  }

  @Test
  public void testCopyReplaceCopiesAndReplacesSuccessfully() {
    collector.checkThat(
        copyReplace(properties, replacements),
        is(both(equalTo(expectedProperties)).and(not(equalTo(properties)))));
  }

  @Parameters
  public static List<Object[]> provideParameters() {
    final Map<ArrowFlightConnectionProperty, Object> data0 = new EnumMap<>(ArrowFlightConnectionProperty.class);
    data0.put(HOST, "127.0.0.1");
    data0.put(PORT, 32010);
    data0.put(PASSWORD, "p455w0rd");
    final Map<ArrowFlightConnectionProperty, Object> data1 = new EnumMap<>(ArrowFlightConnectionProperty.class);
    data1.put(USE_TLS, true);
    final Map<ArrowFlightConnectionProperty, Object> data2 = new EnumMap<>(ArrowFlightConnectionProperty.class);
    data2.put(THREAD_POOL_SIZE, Integer.MAX_VALUE);
    return asList(new Object[][]{{data0}, {data1}, {data2}});
  }
}
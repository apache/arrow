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

import static java.lang.Runtime.getRuntime;
import static java.util.Arrays.asList;
import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.THREAD_POOL_SIZE;
import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.USER;
import static org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty.USE_ENCRYPTION;
import static org.hamcrest.CoreMatchers.is;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.Function;

import org.apache.arrow.driver.jdbc.utils.FlightSqlConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class FlightSqlConnectionConfigImplTest {

  private static final Random RANDOM = new Random(12L);

  private final Properties properties = new Properties();
  private FlightSqlConnectionConfigImpl arrowFlightConnectionConfig;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Parameter
  public ArrowFlightConnectionProperty property;

  @Parameter(value = 1)
  public Object value;

  @Parameter(value = 2)
  public Function<FlightSqlConnectionConfigImpl, ?> arrowFlightConnectionConfigFunction;

  @Before
  public void setUp() {
    arrowFlightConnectionConfig = new FlightSqlConnectionConfigImpl(properties);
    properties.put(property.camelName(), value);
  }

  @Test
  public void testGetProperty() {
    collector.checkThat(arrowFlightConnectionConfigFunction.apply(arrowFlightConnectionConfig),
        is(value));
  }

  @Parameters(name = "<{0}> as <{1}>")
  public static List<Object[]> provideParameters() {
    return asList(new Object[][] {
        {HOST, "host",
            (Function<FlightSqlConnectionConfigImpl, ?>) FlightSqlConnectionConfigImpl::getHost},
        {PORT,
            RANDOM.nextInt(Short.toUnsignedInt(Short.MAX_VALUE)),
            (Function<FlightSqlConnectionConfigImpl, ?>) FlightSqlConnectionConfigImpl::getPort},
        {USER, "user",
            (Function<FlightSqlConnectionConfigImpl, ?>) FlightSqlConnectionConfigImpl::getUser},
        {PASSWORD, "password",
            (Function<FlightSqlConnectionConfigImpl, ?>) FlightSqlConnectionConfigImpl::getPassword},
        {USE_ENCRYPTION, RANDOM.nextBoolean(),
            (Function<FlightSqlConnectionConfigImpl, ?>) FlightSqlConnectionConfigImpl::useEncryption},
        {THREAD_POOL_SIZE,
            RANDOM.nextInt(getRuntime().availableProcessors()),
            (Function<FlightSqlConnectionConfigImpl, ?>) FlightSqlConnectionConfigImpl::threadPoolSize},
    });
  }
}

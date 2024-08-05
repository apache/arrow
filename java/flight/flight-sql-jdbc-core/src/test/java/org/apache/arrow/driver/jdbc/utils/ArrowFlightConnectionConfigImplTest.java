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
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.CATALOG;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.HOST;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PASSWORD;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.PORT;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.THREAD_POOL_SIZE;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USER;
import static org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty.USE_ENCRYPTION;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class ArrowFlightConnectionConfigImplTest {

  private static final Random RANDOM = new Random(12L);

  private Properties properties;
  private ArrowFlightConnectionConfigImpl arrowFlightConnectionConfig;

  public ArrowFlightConnectionProperty property;
  public Object value;
  public Function<ArrowFlightConnectionConfigImpl, ?> arrowFlightConnectionConfigFunction;

  @BeforeEach
  public void setUp() {
    properties = new Properties();
    arrowFlightConnectionConfig = new ArrowFlightConnectionConfigImpl(properties);
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void testGetProperty(
      ArrowFlightConnectionProperty property,
      Object value,
      Function<ArrowFlightConnectionConfigImpl, ?> configFunction) {
    properties.put(property.camelName(), value);
    arrowFlightConnectionConfigFunction = configFunction;
    assertThat(configFunction.apply(arrowFlightConnectionConfig), is(value));
    assertThat(arrowFlightConnectionConfigFunction.apply(arrowFlightConnectionConfig), is(value));
  }

  public static Stream<Arguments> provideParameters() {
    return Stream.of(
        Arguments.of(
            HOST,
            "host",
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::getHost),
        Arguments.of(
            PORT,
            RANDOM.nextInt(Short.toUnsignedInt(Short.MAX_VALUE)),
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::getPort),
        Arguments.of(
            USER,
            "user",
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::getUser),
        Arguments.of(
            PASSWORD,
            "password",
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::getPassword),
        Arguments.of(
            USE_ENCRYPTION,
            RANDOM.nextBoolean(),
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::useEncryption),
        Arguments.of(
            THREAD_POOL_SIZE,
            RANDOM.nextInt(getRuntime().availableProcessors()),
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::threadPoolSize),
        Arguments.of(
            CATALOG,
            "catalog",
            (Function<ArrowFlightConnectionConfigImpl, ?>)
                ArrowFlightConnectionConfigImpl::getCatalog));
  }
}

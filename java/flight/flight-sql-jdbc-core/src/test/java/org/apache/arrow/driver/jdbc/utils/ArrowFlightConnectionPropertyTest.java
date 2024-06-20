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

import static org.apache.arrow.util.AutoCloseables.close;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class ArrowFlightConnectionPropertyTest {

  @Mock public Properties properties;

  private AutoCloseable mockitoResource;

  public ArrowFlightConnectionProperty arrowFlightConnectionProperty;

  @BeforeEach
  public void setUp() {
    mockitoResource = openMocks(this);
  }

  @AfterEach
  public void tearDown() throws Exception {
    close(mockitoResource);
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void testWrapIsUnsupported(ArrowFlightConnectionProperty property) {
    this.arrowFlightConnectionProperty = property;
    ThrowableAssertionUtils.simpleAssertThrowableClass(
        UnsupportedOperationException.class, () -> arrowFlightConnectionProperty.wrap(properties));
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void testRequiredPropertyThrows(ArrowFlightConnectionProperty property) {
    this.arrowFlightConnectionProperty = property;
    assumeTrue(arrowFlightConnectionProperty.required());
    ThrowableAssertionUtils.simpleAssertThrowableClass(
        IllegalStateException.class, () -> arrowFlightConnectionProperty.get(new Properties()));
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void testOptionalPropertyReturnsDefault(ArrowFlightConnectionProperty property) {
    this.arrowFlightConnectionProperty = property;
    assumeTrue(!arrowFlightConnectionProperty.required());
    assertEquals(
        arrowFlightConnectionProperty.defaultValue(),
        arrowFlightConnectionProperty.get(new Properties()));
  }

  public static List<Arguments> provideParameters() {
    final ArrowFlightConnectionProperty[] arrowFlightConnectionProperties =
        ArrowFlightConnectionProperty.values();
    final List<Arguments> parameters = new ArrayList<>(arrowFlightConnectionProperties.length);
    for (final ArrowFlightConnectionProperty arrowFlightConnectionProperty :
        arrowFlightConnectionProperties) {
      parameters.add(Arguments.of(arrowFlightConnectionProperty));
    }
    return parameters;
  }
}

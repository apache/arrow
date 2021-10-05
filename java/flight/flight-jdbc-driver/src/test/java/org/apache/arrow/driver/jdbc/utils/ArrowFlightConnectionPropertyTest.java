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
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.arrow.driver.jdbc.utils.ArrowFlightConnectionConfigImpl.ArrowFlightConnectionProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mock;

@RunWith(Parameterized.class)
public final class ArrowFlightConnectionPropertyTest {

  @Mock
  public Properties properties;

  private AutoCloseable mockitoResource;

  @Parameter
  public ArrowFlightConnectionProperty arrowFlightConnectionProperty;

  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Before
  public void setUp() {
    mockitoResource = openMocks(this);
  }

  @After
  public void tearDown() throws Exception {
    close(mockitoResource);
  }

  @Test
  public void testWrapIsUnsupported() {
    collector.checkThrows(UnsupportedOperationException.class, () -> arrowFlightConnectionProperty.wrap(properties));
  }

  @Parameters
  public static List<Object[]> provideParameters() {
    final ArrowFlightConnectionProperty[] arrowFlightConnectionProperties = ArrowFlightConnectionProperty.values();
    final List<Object[]> parameters = new ArrayList<>(arrowFlightConnectionProperties.length);
    for (final ArrowFlightConnectionProperty arrowFlightConnectionProperty : arrowFlightConnectionProperties) {
      parameters.add(new Object[]{arrowFlightConnectionProperty});
    }
    return parameters;
  }
}

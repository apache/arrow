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

package org.apache.arrow.flight.sql.util;

import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toCollection;
import static org.apache.arrow.flight.sql.util.AdhocTestOption.OPTION_A;
import static org.apache.arrow.flight.sql.util.AdhocTestOption.OPTION_B;
import static org.apache.arrow.flight.sql.util.AdhocTestOption.OPTION_C;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.doesBitmaskTranslateToEnum;
import static org.hamcrest.CoreMatchers.is;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class SqlInfoOptionsUtilsBitmaskParsingTest {

  @Parameter
  public long bitmask;
  @Parameter(value = 1)
  public Set<AdhocTestOption> expectedOptions;
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Parameters
  public static List<Object[]> provideParameters() {
    return asList(
        new Object[][]{
            {0L, EnumSet.noneOf(AdhocTestOption.class)},
            {1L, EnumSet.of(OPTION_A)},
            {0b10L, EnumSet.of(OPTION_B)},
            {0b11L, EnumSet.of(OPTION_A, OPTION_B)},
            {0b100L, EnumSet.of(OPTION_C)},
            {0b101L, EnumSet.of(OPTION_A, OPTION_C)},
            {0b110L, EnumSet.of(OPTION_B, OPTION_C)},
            {0b111L, EnumSet.allOf(AdhocTestOption.class)},
        });
  }

  @Test
  public void testShouldFilterOutEnumsBasedOnBitmask() {
    final Set<AdhocTestOption> actualOptions =
        stream(AdhocTestOption.values())
            .filter(enumInstance -> doesBitmaskTranslateToEnum(enumInstance, bitmask))
            .collect(toCollection(() -> EnumSet.noneOf(AdhocTestOption.class)));
    collector.checkThat(actualOptions, is(expectedOptions));
  }
}

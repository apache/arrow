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
import static org.apache.arrow.flight.sql.util.AdhocTestOption.OPTION_A;
import static org.apache.arrow.flight.sql.util.AdhocTestOption.OPTION_B;
import static org.apache.arrow.flight.sql.util.AdhocTestOption.OPTION_C;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.createBitmaskFromEnums;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public final class SqlInfoOptionsUtilsBitmaskCreationTest {

  public static List<Object[]> provideParameters() {
    return asList(
        new Object[][]{
            {new AdhocTestOption[0], 0L},
            {new AdhocTestOption[]{OPTION_A}, 1L},
            {new AdhocTestOption[]{OPTION_B}, 0b10L},
            {new AdhocTestOption[]{OPTION_A, OPTION_B}, 0b11L},
            {new AdhocTestOption[]{OPTION_C}, 0b100L},
            {new AdhocTestOption[]{OPTION_A, OPTION_C}, 0b101L},
            {new AdhocTestOption[]{OPTION_B, OPTION_C}, 0b110L},
            {AdhocTestOption.values(), 0b111L},
        });
  }

  @ParameterizedTest
  @MethodSource("provideParameters")
  public void testShouldBuildBitmaskFromEnums(
      AdhocTestOption[] adhocTestOptions, long expectedBitmask
  ) {
    Assertions.assertEquals(createBitmaskFromEnums(adhocTestOptions), expectedBitmask);
  }
}

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

import java.util.Arrays;
import java.util.Collection;

import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.flight.sql.impl.FlightSql.SqlInfo;

import com.google.protobuf.ProtocolMessageEnum;

/**
 * Utility class for {@link SqlInfo} and {@link FlightSqlClient#getSqlInfo} option parsing.
 */
public final class SqlInfoOptionsUtils {
  private SqlInfoOptionsUtils() {
    // Prevent instantiation.
  }

  /**
   * Returns whether the provided {@code bitmask} points to the provided {@link ProtocolMessageEnum} by comparing
   * {@link ProtocolMessageEnum#getNumber} with the respective bit index of the {@code bitmask}.
   *
   * @param enumInstance the protobuf message enum to use.
   * @param bitmask      the bitmask response from {@link FlightSqlClient#getSqlInfo}.
   * @return whether the provided {@code bitmask} points to the specified {@code enumInstance}.
   */
  public static boolean doesBitmaskTranslateToEnum(final ProtocolMessageEnum enumInstance, final long bitmask) {
    return ((bitmask >> enumInstance.getNumber()) & 1) == 1;
  }

  /**
   * Creates a bitmask that translates to the specified {@code enums}.
   *
   * @param enums the {@link ProtocolMessageEnum} instances to represent as bitmask.
   * @return the bitmask.
   */
  public static long createBitmaskFromEnums(final ProtocolMessageEnum... enums) {
    return createBitmaskFromEnums(Arrays.asList(enums));
  }

  /**
   * Creates a bitmask that translates to the specified {@code enums}.
   *
   * @param enums the {@link ProtocolMessageEnum} instances to represent as bitmask.
   * @return the bitmask.
   */
  public static long createBitmaskFromEnums(final Collection<ProtocolMessageEnum> enums) {
    return enums.stream()
        .mapToInt(ProtocolMessageEnum::getNumber)
        .map(bitIndexToSet -> 1 << bitIndexToSet)
        .reduce((firstBitmask, secondBitmask) -> firstBitmask | secondBitmask)
        .orElse(0);
  }
}

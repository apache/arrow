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

import java.sql.SQLException;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.junit.Assert;
import org.junit.Test;

public class FlightToJDBCExceptionMapperTest {

  @Test
  public void unauthenticatedStateMappingTest() {
    assertMapStates(CallStatus.UNAUTHENTICATED, "28000");
  }

  @Test
  public void unauthorisedStateMappingTest() {
    assertMapStates(CallStatus.UNAUTHORIZED, "42000");
  }

  @Test
  public void unavailableStateMappingTest() {
    assertMapStates(CallStatus.UNAVAILABLE, "08001");
  }

  @Test
  public void unimplementedStateMappingTest() {
    assertMapStates(CallStatus.UNIMPLEMENTED, "0A000");
  }

  @Test
  public void canceledStateMappingTest() {
    assertMapStates(CallStatus.CANCELLED, "HY008");
  }

  @Test
  public void alreadyExistsStateMappingTest() {
    assertMapStates(CallStatus.ALREADY_EXISTS, "21000");
  }

  @Test
  public void notFoundStateMappingTest() {
    assertMapStates(CallStatus.NOT_FOUND, "42000");
  }

  @Test
  public void timeOutStateMappingTest() {
    assertMapStates(CallStatus.TIMED_OUT, "HYT01");
  }

  private void assertMapStates(CallStatus timedOut, String code) {
    FlightRuntimeException flightRuntimeException =
        timedOut.withDescription("Failure in connection: %s").toRuntimeException();
    SQLException map =
        FlightToJDBCExceptionMapper.map(flightRuntimeException);
    Assert.assertEquals(code, map.getSQLState());
  }

  @Test
  public void invalidArgumentStateMappingTest() {
    assertMapStates(CallStatus.INVALID_ARGUMENT, "2200T");
  }

  @Test
  public void internalStateMappingTest() {
    assertMapStates(CallStatus.INTERNAL, "01000");
  }

  @Test
  public void unknownStateMappingTest() {
    assertMapStates(CallStatus.UNKNOWN, "01000");
  }
}

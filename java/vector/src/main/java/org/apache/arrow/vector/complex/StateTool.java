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

package org.apache.arrow.vector.complex;

import java.util.Arrays;

/**
 * Utility methods for state machines based on enums.
 */
public class StateTool {
  private StateTool() {}

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StateTool.class);

  /**
   * Verifies <code>currentState</code> is in one of <code>expectedStates</code>,
   * throws an IllegalArgumentException if it isn't.
   */
  public static <T extends Enum<?>> void check(T currentState, T... expectedStates) {
    for (T s : expectedStates) {
      if (s == currentState) {
        return;
      }
    }
    throw new IllegalArgumentException(String.format("Expected to be in one of these states %s but was actually in " +
      "state %s", Arrays.toString(expectedStates), currentState));
  }

}

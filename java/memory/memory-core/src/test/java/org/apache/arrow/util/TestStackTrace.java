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

package org.apache.arrow.util;

import org.apache.arrow.memory.util.StackTrace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStackTrace {
  /**
   * Check that the stack trace includes the origin line.
   */
  @Test
  public void testStackTraceComplete() {
    final String stackTrace = new StackTrace().toString();
    Assertions.assertTrue(stackTrace.contains("TestStackTrace.testStackTraceComplete"), stackTrace);
  }

  /**
   * Check that the stack trace doesn't include getStackTrace or StackTrace.
   */
  @Test
  public void testStackTraceOmit() {
    final String stackTrace = new StackTrace().toString();
    Assertions.assertFalse(stackTrace.contains("Thread.getStackTrace"), stackTrace);
    Assertions.assertFalse(stackTrace.contains("org.apache.arrow.memory.util.StackTrace"), stackTrace);
  }
}

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

package org.apache.arrow.memory.util;

import org.checkerframework.dataflow.qual.AssertMethod;

/**
 * Utility methods for handling global configuration into CheckerFramework library.
 */
public class CheckerFrameworkUtil {

  /**
   * Method annotation @AssertMethod indicates that a method checks a value and possibly
   * throws an assertion. Using it can make flow-sensitive type refinement more effective.
   *
   * @param eval The assertion to be evaluated at compile time
   */
  @AssertMethod
  public static void assertMethod(boolean eval) {
    if (!eval) {
      throw new NullPointerException();
    }
  }
}

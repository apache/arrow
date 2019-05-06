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

package org.apache.arrow.memory;

/**
 * Configuration class to determine if bounds checking should be turned on or off.
 *
 * <p>Bounds checking is on by default.  To disable it you must set the system properties
 * "arrow.enable_unsafe_memory_access" and "drill.enable_unsafe_memory_access" to "true"
 * and disable java assertions.
 */
public class BoundsChecking {

  public static boolean BOUNDS_CHECKING_ENABLED;
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoundsChecking.class);

  static {
    evaluate();
  }

  public static void evaluate() {
    String oldProperty = System.getProperty("drill.enable_unsafe_memory_access");
    if (oldProperty != null) {
      logger.warn("\"drill.enable_unsafe_memory_access\" has been renamed to \"arrow.enable_unsafe_memory_access\"");
      logger.warn("\"arrow.enable_unsafe_memory_access\" can be set to: " +
              " true (to not check) or false (to check, default)");
    }

    String newProperty = System.getProperty("arrow.enable_unsafe_memory_access");

    // new property has higher priority
    String unsafeProperty = newProperty != null ? newProperty : oldProperty;
    BOUNDS_CHECKING_ENABLED = !"true".equals(unsafeProperty);
  }

  private BoundsChecking() {
  }

}

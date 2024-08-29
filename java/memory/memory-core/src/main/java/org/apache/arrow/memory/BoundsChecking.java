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
 * <p>
 * Bounds checking is on by default.  You can disable it by setting either the system property or
 * the environmental variable to "true". The system property can be "arrow.enable_unsafe_memory_access"
 * or "drill.enable_unsafe_memory_access". The latter is deprecated. The environmental variable is named
 * "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS".
 * When both the system property and the environmental variable are set, the system property takes precedence.
 * </p>
 */
public class BoundsChecking {

  public static final boolean BOUNDS_CHECKING_ENABLED;
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoundsChecking.class);

  static {
    String envProperty = System.getenv("ARROW_ENABLE_UNSAFE_MEMORY_ACCESS");
    String oldProperty = System.getProperty("drill.enable_unsafe_memory_access");
    if (oldProperty != null) {
      logger.warn("\"drill.enable_unsafe_memory_access\" has been renamed to \"arrow.enable_unsafe_memory_access\"");
      logger.warn("\"arrow.enable_unsafe_memory_access\" can be set to: " +
              " true (to not check) or false (to check, default)");
    }
    String newProperty = System.getProperty("arrow.enable_unsafe_memory_access");

    // The priority of determining the unsafe flag:
    // 1. The system properties take precedence over the environmental variable.
    // 2. The new system property takes precedence over the new system property.
    String unsafeFlagValue = newProperty;
    if (unsafeFlagValue == null) {
      unsafeFlagValue = oldProperty;
    }
    if (unsafeFlagValue == null) {
      unsafeFlagValue = envProperty;
    }

    BOUNDS_CHECKING_ENABLED = !"true".equals(unsafeFlagValue);
  }

  private BoundsChecking() {
  }

}

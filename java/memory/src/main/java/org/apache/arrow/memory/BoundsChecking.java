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

public class BoundsChecking {

  public static final boolean BOUNDS_CHECKING_ENABLED;
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BoundsChecking.class);

  static {
    String oldProperty = System.getProperty("drill.enable_unsafe_memory_access");
    if (oldProperty != null) {
      logger.warn("\"drill.enable_unsafe_memory_access\" has been renamed to \"arrow.enable_unsafe_memory_access\"");
      logger.warn("\"arrow.enable_unsafe_memory_access\" can be set to: " +
              " true (to not check) or false (to check, default)");
    }
    boolean isAssertEnabled = false;
    assert isAssertEnabled = true;
    BOUNDS_CHECKING_ENABLED = isAssertEnabled ||
      !"true".equals(System.getProperty("arrow.enable_unsafe_memory_access")) ||
      !"true".equals(oldProperty);
  }

  private BoundsChecking() {
  }

}

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
 * Configuration class to determine if null checking should be enabled or disabled for the "get" methods.
 * For example, the get method of class org.apache.arrow.vector.Float8Vector first checks if the value
 * at the given index is null, before retrieving the value. This configuration will turn on and off such checks.
 *
 * <p>Null checking is on by default. You can disable it by setting either the system property or the
 * environmental variable to "false". The system property is named "arrow.enable_null_check_for_get" and
 * the environmental variable is named "ARROW_ENABLE_NULL_CHECK_FOR_GET".
 * When both the system property and the environmental variable are set, the system property takes precedence.
 * </p>
 */
public class NullCheckingForGet {

  /**
   * The flag to indicate if null checking is enabled for "get" methods.
   */
  public static final boolean NULL_CHECKING_ENABLED;

  static {
    String envProperty = System.getenv("ARROW_ENABLE_NULL_CHECK_FOR_GET");
    String sysProperty = System.getProperty("arrow.enable_null_check_for_get");

    // The system property has a higher priority than the environmental variable.
    String flagValue = sysProperty;
    if (flagValue == null) {
      flagValue = envProperty;
    }

    // The flag is set to false only if the system property/environmental
    // variable is explicitly set to "false".
    NULL_CHECKING_ENABLED = !"false".equals(flagValue);
  }

  private NullCheckingForGet() {
  }
}

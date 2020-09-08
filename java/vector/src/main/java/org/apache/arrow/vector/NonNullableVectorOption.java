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

package org.apache.arrow.vector;

/**
 * Configuration class to determine if non-nullable vectors are enabled or not.
 *
 * <p>
 * Non-nullable vectors are disabled by default.  You can enable it by setting either the system property or
 * the environmental variable to "true". The system property name is "arrow.enable_non_nullable_vectors".
 * The environmental variable name is "ARROW_ENABLE_NON_NULLABLE_VECTORS".
 * When both the system property and the environmental variable are set, the system property takes precedence.
 * </p>
 */
public class NonNullableVectorOption {

  public static final boolean NON_NULLABLE_VECTORS_ENABLED;
  static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(NonNullableVectorOption.class);

  static {
    String envProperty = System.getenv("ARROW_ENABLE_NON_NULLABLE_VECTORS");
    String sysProperty = System.getProperty("arrow.enable_non_nullable_vectors");

    // The system property has a higher priority than the environmental variable.
    String flagValue = sysProperty;
    if (flagValue == null) {
      flagValue = envProperty;
    }

    // The flag is set to true only if the system property/environmental
    // variable is explicitly set to "true".
    NON_NULLABLE_VECTORS_ENABLED = "true".equals(flagValue);
    LOGGER.info("Set NON_NULLABLE_VECTORS_ENABLED to " + NON_NULLABLE_VECTORS_ENABLED);
  }

  private NonNullableVectorOption() {
  }
}

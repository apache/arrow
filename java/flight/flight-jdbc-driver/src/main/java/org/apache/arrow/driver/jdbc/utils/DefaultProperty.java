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

import java.util.AbstractMap;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * An enum for centralizing default property names.
 */
public enum BaseProperty {
  // TODO These names are up to discussion.
  HOST("host", "localhost"), PORT("port", 32210), USERNAME("user"), PASSWORD(
      "password"), ENCRYPT("useTls",
          false), KEYSTORE_PATH("keyStorePath"), KEYSTORE_PASS("keyStorePass"),
  THREAD_POOL_SIZE("threadPoolSize", 1);

  private final String representation;
  private final Object definition;

  BaseProperty(final String representation, @Nullable final Object definition) {
    this.representation = representation;
    this.definition = definition;
  }

  BaseProperty(final String representation) {
    this.representation = representation;
    this.definition = null;
  }

  /**
   * Gets the {@link Map.Entry} representation of this property, where
   * {@link Map.Entry#getKey} gets the name and {@link Map.Entry#getValue} gets
   * the default value of this property, or {@code null} if it lacks one.
   *
   * @return the entry of this property.
   */
  public Map.Entry<String, Object> getEntry() {

    /*
     * FIXME Should the second parameter be wrapped as an Optional?
     *
     * It's probably a better idea to make this return a
     * Map.Entry<String, Optional<Object>> instead, for the following reasons:
     *  - 1. It avoids having to null-check constantly, and;
     *  - 2. What if the default value IS null? (As opposed to null meaning
     *  there is no default value.)
     */
    return new AbstractMap.SimpleEntry<>(representation, definition);
  }
}

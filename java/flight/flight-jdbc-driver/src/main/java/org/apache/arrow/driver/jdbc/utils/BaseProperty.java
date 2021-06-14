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

/**
 * An enum for centralizing default property names.
 */
public enum BaseProperty {
  // TODO These names are up to discussion.
  HOST("host", "localhost"), PORT("port", 32210), USERNAME("user"), PASSWORD(
      "password"), ENCRYPT("useTls",
          false), KEYSTORE_PATH("keyStorePath"), KEYSTORE_PASS("keyStorePass");

  private final String repr;
  private Object def;

  BaseProperty(final String repr, final Object def) {
    this(repr);
    this.def = def;
  }

  BaseProperty(final String repr) {
    this.repr = repr;
  }

  /**
   * Gets the {@link Map.Entry} representation of this property, where
   * {@link Map.Entry#getKey} gets the name and {@link Map.Entry#getValue} gets
   * the default value of this property, or {@code null} if it lacks one.
   *
   * @return the entry of this property.
   */
  public Map.Entry<Object, Object> getEntry() {
    return new AbstractMap.SimpleEntry<>(repr, def);
  }
}

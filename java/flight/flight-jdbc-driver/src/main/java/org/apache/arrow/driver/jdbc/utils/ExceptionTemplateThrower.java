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

import static java.lang.String.format;

import org.apache.calcite.avatica.util.Cursor.Accessor;

/**
 * Utility class for managing exceptions thrown by
 * {@link Accessor}s.
 */
public final class ExceptionTemplateThrower {

  private ExceptionTemplateThrower() {
    // Prevent instantiation.
  }

  /**
   * Gets a {@link Exception} for an attempt to perform a conversion
   * not yet supported by the {@link Accessor} in use.
   *
   * @return the exception.
   */
  public static UnsupportedOperationException getOperationNotSupported(final Class<?> type) {
    return new UnsupportedOperationException(
            format("Operation not supported for type: %s.", type.getName()));
  }
}

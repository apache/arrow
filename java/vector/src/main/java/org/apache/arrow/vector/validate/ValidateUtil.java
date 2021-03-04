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

package org.apache.arrow.vector.validate;

/**
 * Utilities for vector validation.
 */
public class ValidateUtil {

  private ValidateUtil() {
  }

  /**
   * Validate the expression.
   * @param expression the expression to validate.
   * @param errorMessage the error message.
   * @throws ValidateException if the expression evaluates to false.
   */
  public static void validateOrThrow(boolean expression, String errorMessage) {
    if (!expression) {
      throw new ValidateException(errorMessage);
    }
  }

  /**
   * Validate the expression.
   * @param expression the expression to validate.
   * @param errorMessage the error message template.
   * @param args the error message arguments.
   * @throws ValidateException if the expression evaluates to false.
   */
  public static void validateOrThrow(boolean expression, String errorMessage, Object... args) {
    if (!expression) {
      throw new ValidateException(String.format(errorMessage, args));
    }
  }

  /**
   * A exception that is thrown when the vector validation fails.
   */
  public static class ValidateException extends RuntimeException {
    public ValidateException(String message) {
      super(message);
    }
  }
}

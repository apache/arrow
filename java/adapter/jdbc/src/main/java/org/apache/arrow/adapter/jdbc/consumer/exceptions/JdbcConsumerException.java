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

package org.apache.arrow.adapter.jdbc.consumer.exceptions;

import org.apache.arrow.adapter.jdbc.JdbcFieldInfo;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Exception while consuming JDBC data. This exception stores the JdbcFieldInfo for the column and the
 * ArrowType for the corresponding vector for easier debugging.
 */
public class JdbcConsumerException extends RuntimeException {
  final JdbcFieldInfo fieldInfo;
  final ArrowType arrowType;

  /**
   * Construct JdbcConsumerException with all fields.
   *
   * @param message   error message
   * @param cause     original exception
   * @param fieldInfo JdbcFieldInfo for the column
   * @param arrowType ArrowType for the corresponding vector
   */
  public JdbcConsumerException(String message, Throwable cause, JdbcFieldInfo fieldInfo, ArrowType arrowType) {
    super(message, cause);
    this.fieldInfo = fieldInfo;
    this.arrowType = arrowType;
  }

  public ArrowType getArrowType() {
    return this.arrowType;
  }

  public JdbcFieldInfo getFieldInfo() {
    return this.fieldInfo;
  }
}

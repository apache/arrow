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

package org.apache.arrow.driver.jdbc.accessor.impl;

import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessor;
import org.apache.arrow.driver.jdbc.accessor.ArrowFlightJdbcAccessorFactory;
import org.apache.arrow.vector.NullVector;

/**
 * Accessor for the Arrow type {@link NullVector}.
 */
public class ArrowFlightJdbcNullVectorAccessor extends ArrowFlightJdbcAccessor {
  public ArrowFlightJdbcNullVectorAccessor(
      ArrowFlightJdbcAccessorFactory.WasNullConsumer setCursorWasNull) {
    super(null, setCursorWasNull);
  }

  @Override
  public Class<?> getObjectClass() {
    return Object.class;
  }

  @Override
  public boolean wasNull() {
    return true;
  }

  @Override
  public Object getObject() {
    this.wasNullConsumer.setWasNull(true);
    return null;
  }
}

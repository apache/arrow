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

package org.apache.arrow.driver.jdbc;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.StatementHandle;

/**
 * A SQL statement for querying data from an Arrow Flight server.
 */
public class ArrowFlightStatement extends AvaticaStatement {

  public ArrowFlightStatement(final AvaticaConnection connection,
      final StatementHandle handle, final int resultSetType,
      final int resultSetConcurrency, final int resultSetHoldability) {
    super(connection, handle, resultSetType, resultSetConcurrency,
        resultSetHoldability);
  }

  public ArrowFlightStatement(final AvaticaConnection connection,
      final StatementHandle handle, final int resultSetType,
      final int resultSetConcurrency, final int resultSetHoldability,
      final Signature signature) {
    super(connection, handle, resultSetType, resultSetConcurrency,
        resultSetHoldability, signature);
  }

}

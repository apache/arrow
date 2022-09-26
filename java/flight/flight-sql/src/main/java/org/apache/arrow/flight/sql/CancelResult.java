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

package org.apache.arrow.flight.sql;

import org.apache.arrow.flight.sql.impl.FlightSql;

/**
 * The result of cancelling a query.
 */
public enum CancelResult {
  UNSPECIFIED,
  CANCELLED,
  CANCELLING,
  NOT_CANCELLABLE,
  ;

  FlightSql.ActionCancelQueryResult.CancelResult toProtocol() {
    switch (this) {
      default:
      case UNSPECIFIED:
        return FlightSql.ActionCancelQueryResult.CancelResult.CANCEL_RESULT_UNSPECIFIED;
      case CANCELLED:
        return FlightSql.ActionCancelQueryResult.CancelResult.CANCEL_RESULT_CANCELLED;
      case CANCELLING:
        return FlightSql.ActionCancelQueryResult.CancelResult.CANCEL_RESULT_CANCELLING;
      case NOT_CANCELLABLE:
        return FlightSql.ActionCancelQueryResult.CancelResult.CANCEL_RESULT_NOT_CANCELLABLE;
    }
  }
}

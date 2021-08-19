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

package org.apache.arrow.driver.jdbc.client;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;

/**
 * A wrapper for a {@link FlightSqlClient}.
 */
public interface FlightSqlClientHandler extends BareFlightClientHandler {

  /**
   * Gets the {@link FlightSqlClient} wrapped by this handler.
   *
   * @return the client.
   */
  FlightSqlClient getSqlClient();

  @Override
  default List<FlightStream> getStreams(final String query) {
    return getInfo(query).getEndpoints().stream()
        .map(FlightEndpoint::getTicket)
        .map(ticket -> getSqlClient().getStream(ticket, getOptions().toArray(new CallOption[0])))
        .collect(Collectors.toList());
  }

  @Override
  default FlightInfo getInfo(final String query) {
    // FIXME Throws `FlightRuntimeException`: UNAUTHENTICATED
    // TODO Implement `FlightSqlClient#execute(String, CallOption[])`
    return getSqlClient().execute(query /*, getOptions().toArray(new CallOption[0]) */);
  }
}

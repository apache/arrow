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

package org.apache.arrow.driver.jdbc.client.impl;

import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.arrow.driver.jdbc.client.ArrowFlightClientHandler;
import org.apache.arrow.driver.jdbc.client.FlightClientHandler;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;

/**
 * A {@link FlightClientHandler} for a {@link FlightSqlClient}.
 */
public final class ArrowFlightSqlClientHandler extends ArrowFlightClientHandler {

  private final FlightSqlClient sqlClient;

  public ArrowFlightSqlClientHandler(final FlightSqlClient sqlClient,
                                     final CallOption... options) {
    super(options);
    this.sqlClient = Preconditions.checkNotNull(sqlClient);
  }

  /**
   * Creates a new {@link ArrowFlightSqlClientHandler} from the provided {@code clientInfo}.
   *
   * @param clientInfo the {@link FlightClient} to manage along with {@link CallOption}s to use in subsequent calls.
   * @return a new {@link FlightClientHandler}.
   */
  public static FlightClientHandler createNewHandler(final Entry<FlightClient, CallOption[]> clientInfo) {
    return createNewHandler(clientInfo.getKey(), clientInfo.getValue());
  }

  /**
   * Creates a new {@link ArrowFlightSqlClientHandler} from the provided {@code client} and {@code options}.
   *
   * @param client  the {@link FlightClient} to manage under a {@link FlightSqlClient} wrapper.
   * @param options the {@link CallOption}s to persist in between subsequent client calls.
   * @return a new {@link FlightClientHandler}.
   */
  public static FlightClientHandler createNewHandler(final FlightClient client, final CallOption... options) {
    return new ArrowFlightSqlClientHandler(new FlightSqlClient(client), options);
  }

  @Override
  public List<FlightStream> getStreams(String query) {
    return getInfo(query).getEndpoints().stream()
        .map(FlightEndpoint::getTicket)
        .map(ticket -> sqlClient.getStream(ticket, getOptions()))
        .collect(Collectors.toList());
  }

  @Override
  public FlightInfo getInfo(String query) {
    return sqlClient.execute(query, getOptions());
  }

  @Override
  public void close() throws SQLException {
    try {
      AutoCloseables.close(sqlClient);
    } catch (final Exception e) {
      throw new SQLException("Failed to clean up client resources.", e);
    }
  }
}

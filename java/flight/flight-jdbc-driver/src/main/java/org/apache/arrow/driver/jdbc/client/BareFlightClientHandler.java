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

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.util.AutoCloseables;
import org.apache.calcite.avatica.AvaticaConnection;

/**
 * An adhoc {@link FlightClient} wrapper, used to access the client. Allows for
 * the reuse of credentials and properties.
 */
public interface BareFlightClientHandler extends FlightClientHandler {

  /**
   * Gets the {@link FlightClient} wrapped by this handler.
   *
   * @return the client.
   */
  FlightClient getClient();

  @Override
  default FlightInfo getInfo(final String query) {
    return getClient().getInfo(
        FlightDescriptor.command(query.getBytes(StandardCharsets.UTF_8)),
        getOptions().toArray(new CallOption[0]));
  }

  @Override
  default List<FlightStream> getStreams(final String query) {
    return getInfo(query).getEndpoints().stream()
        .map(FlightEndpoint::getTicket)
        .map(ticket -> getClient().getStream(ticket, getOptions().toArray(new CallOption[0])))
        .collect(Collectors.toList());
  }

  @Override
  default void close() throws SQLException {
    try {
      AutoCloseables.close(getClient());
    } catch (final Exception e) {
      throw AvaticaConnection.HELPER.createException("Failed to close resources.", e);
    }
  }
}

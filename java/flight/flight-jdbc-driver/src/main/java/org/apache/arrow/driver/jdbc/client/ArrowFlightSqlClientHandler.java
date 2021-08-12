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

import java.util.Arrays;
import java.util.Collection;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.util.Preconditions;

/**
 * Wrapper for a {@link FlightSqlClient}.
 */
public class ArrowFlightSqlClientHandler extends ArrowFlightClientHandler implements FlightSqlClientHandler {

  private final FlightSqlClient sqlClient;

  protected ArrowFlightSqlClientHandler(final FlightClient client, final FlightSqlClient sqlClient,
                                        final CallOption... options) {
    this(client, sqlClient, Arrays.asList(options));
  }

  protected ArrowFlightSqlClientHandler(final FlightClient client, final FlightSqlClient sqlClient,
                                        final Collection<CallOption> options) {
    super(client, options);
    this.sqlClient = Preconditions.checkNotNull(sqlClient);
  }

  /**
   * Instantiates a new {@link ArrowFlightSqlClientHandler} wrapping the provided {@code client}.
   *
   * @param client  the client to wrap.
   * @param options the options for subsequent calls.
   * @return a new handler.
   */
  public static ArrowFlightSqlClientHandler createNewHandler(final FlightClient client,
                                                             final CallOption... options) {
    return new ArrowFlightSqlClientHandler(client, new FlightSqlClient(client), options);
  }

  @Override
  public final FlightSqlClient getSqlClient() {
    return sqlClient;
  }
}

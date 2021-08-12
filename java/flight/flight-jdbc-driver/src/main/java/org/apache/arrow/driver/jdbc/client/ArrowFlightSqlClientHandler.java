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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.util.Preconditions;

/**
 * Wrapper for a {@link FlightSqlClient}.
 */
public class ArrowFlightSqlClientHandler implements FlightSqlClientHandler {

  private final FlightSqlClient client;
  private final List<CallOption> options = new ArrayList<>();

  public ArrowFlightSqlClientHandler(final FlightSqlClient client, final CallOption... options) {
    this(client, Arrays.asList(options));
  }

  public ArrowFlightSqlClientHandler(final FlightSqlClient client, final Collection<CallOption> options) {
    this.client = Preconditions.checkNotNull(client);
    this.options.addAll(options);
  }

  @Override
  public final List<CallOption> getOptions() {
    return options;
  }

  @Override
  public final FlightSqlClient getClient() {
    return client;
  }
}

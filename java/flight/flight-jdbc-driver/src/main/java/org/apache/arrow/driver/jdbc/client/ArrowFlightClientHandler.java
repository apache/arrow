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
import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.util.Preconditions;

/**
 * Handler for a {@link FlightClient}.
 */
public abstract class ArrowFlightClientHandler implements FlightClientHandler {
  private final Set<CallOption> options = new HashSet<>();
  private final FlightClient client;

  protected ArrowFlightClientHandler(final FlightClient client, final CallOption... options) {
    this(client, Arrays.asList(options));
  }

  protected ArrowFlightClientHandler(final FlightClient client, final Collection<CallOption> options) {
    this.client = Preconditions.checkNotNull(client);
    this.options.addAll(options);
  }

  @Override
  public final FlightClient getClient() {
    return client;
  }

  @Override
  public final CallOption[] getOptions() {
    return options.toArray(new CallOption[0]);
  }
}

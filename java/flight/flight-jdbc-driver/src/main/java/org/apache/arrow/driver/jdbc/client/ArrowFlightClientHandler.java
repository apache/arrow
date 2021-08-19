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
import org.apache.arrow.flight.FlightClient;

/**
 * A wrapper for a {@link FlightClient}.
 */
public abstract class ArrowFlightClientHandler implements FlightClientHandler {
  private final List<CallOption> options = new ArrayList<>();

  protected ArrowFlightClientHandler(final CallOption... options) {
    this(Arrays.asList(options));
  }

  protected ArrowFlightClientHandler(final Collection<CallOption> options) {
    this.options.addAll(options);
  }

  @Override
  public final List<CallOption> getOptions() {
    return options;
  }
}

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

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.util.Preconditions;

/**
 * Represents a connection to a {@link org.apache.arrow.flight.FlightEndpoint}.
 */
public class CloseableEndpointStreamPair implements AutoCloseable {

  private final FlightStream stream;
  private final FlightSqlClient client;

  public CloseableEndpointStreamPair(FlightStream stream, FlightSqlClient client) {
    this.stream = Preconditions.checkNotNull(stream);
    this.client = client;
  }

  public FlightStream getStream() {
    return stream;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(stream, client);
  }
}

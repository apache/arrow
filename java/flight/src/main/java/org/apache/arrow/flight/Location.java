/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import org.apache.arrow.flight.impl.Flight;

public class Location {

  private final String host;
  private final int port;

  public Location(String host, int port) {
    super();
    this.host = host;
    this.port = port;
  }

  Location(Flight.Location location) {
    this.host = location.getHost();
    this.port = location.getPort();
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  Flight.Location toProtocol() {
    return Flight.Location.newBuilder().setHost(host).setPort(port).build();
  }

}

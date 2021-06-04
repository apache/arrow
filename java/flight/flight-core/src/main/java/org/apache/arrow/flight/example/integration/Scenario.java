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

package org.apache.arrow.flight.integration.tests;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

/**
 * A particular scenario in integration testing.
 */
interface Scenario {

  /**
   * Construct the FlightProducer for a server in this scenario.
   */
  FlightProducer producer(BufferAllocator allocator, Location location) throws Exception;

  /**
   * Set any other server options.
   */
  void buildServer(FlightServer.Builder builder) throws Exception;

  /**
   * Run as the client in the scenario.
   */
  void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception;
}

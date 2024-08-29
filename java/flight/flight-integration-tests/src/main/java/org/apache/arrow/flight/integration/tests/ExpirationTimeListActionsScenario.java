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

import java.util.Iterator;

import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;

/** Test ListActions with expiration times. */
final class ExpirationTimeListActionsScenario implements Scenario {
  @Override
  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new ExpirationTimeProducer(allocator);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    Iterator<ActionType> actions = client.listActions().iterator();
    IntegrationAssertions.assertTrue("Expected 2 actions", actions.hasNext());
    ActionType action = actions.next();
    IntegrationAssertions.assertEquals(FlightConstants.CANCEL_FLIGHT_INFO.getType(), action.getType());

    IntegrationAssertions.assertTrue("Expected 2 actions", actions.hasNext());
    action = actions.next();
    IntegrationAssertions.assertEquals(FlightConstants.RENEW_FLIGHT_ENDPOINT.getType(), action.getType());

    IntegrationAssertions.assertFalse("Expected 2 actions", actions.hasNext());
  }
}

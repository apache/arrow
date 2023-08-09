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

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.PollInfo;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/** Test PollFlightInfo. */
class PollFlightInfoProducer extends NoOpFlightProducer {
  static final byte[] POLL_DESCRIPTOR = "poll".getBytes(StandardCharsets.UTF_8);

  @Override
  public PollInfo pollFlightInfo(CallContext context, FlightDescriptor descriptor) {
    Schema schema = new Schema(
        Collections.singletonList(Field.notNullable("number", Types.MinorType.UINT4.getType())));
    List<FlightEndpoint> endpoints = Collections.singletonList(
        new FlightEndpoint(
            new Ticket("long-running query".getBytes(StandardCharsets.UTF_8))));
    FlightInfo info = new FlightInfo(schema, descriptor, endpoints, -1, -1 );
    if (descriptor.isCommand() && Arrays.equals(descriptor.getCommand(), POLL_DESCRIPTOR)) {
      return new PollInfo(info, null, 1.0, null);
    } else {
      return new PollInfo(
          info, FlightDescriptor.command(POLL_DESCRIPTOR), 0.1, Instant.now().plus(10, ChronoUnit.SECONDS));
    }
  }
}

package org.apache.arrow.flight.integration.tests;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;

final class SessionOptionsScenario implements Scenario {
  // TODO this needs to be at Scenario scope (& not static)
  private final FlightServerMiddleware.Key<ServerSessionMiddleware> key =
    FlightServerMiddleware.Key.of("sessionmiddleware");

  public FlightProducer producer(BufferAllocator allocator, Location location) throws Exception {
    return new SessionOptionsProducer(key);
  }

  @Override
  public void buildServer(FlightServer.Builder builder) {
    builder.middleware(key, new ServerSessionMiddleware.Factory());
  }

  @Override
  public void client(BufferAllocator allocator, Location location, FlightClient client) throws Exception {
    // TODO PHOXME
  }
}
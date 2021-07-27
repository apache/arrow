package org.apache.arrow.driver.jdbc.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.flight.FlightStream;

public class FlightStreamQueue implements AutoCloseable {
  private static final int THREAD_POOL_SIZE = 4;

  private final ExecutorService executorService;
  private final BlockingQueue<FlightStream> flightStreamQueue;
  private final AtomicInteger pendingFutures;

  public FlightStreamQueue() {
    executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    flightStreamQueue = new LinkedBlockingQueue<>();
    pendingFutures = new AtomicInteger(0);
  }

  public FlightStream next() {
    try {
      while (pendingFutures.getAndDecrement() > 0) {
        final FlightStream flightStream = flightStreamQueue.take();
        if (flightStream.getRoot().getRowCount() > 0) {
          return flightStream;
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    return null;
  }

  public void addToQueue(FlightStream flightStream) {
    pendingFutures.incrementAndGet();
    executorService.submit(() -> {
      // FlightStream#next will block until new data can be read or stream is over.
      flightStream.next();
      flightStreamQueue.add(flightStream);
    });
  }

  @Override
  public void close() throws Exception {
    this.executorService.shutdown();
  }
}

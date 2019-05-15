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

package org.apache.arrow.flight;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;
import java.util.function.Function;

/**
 * Utility methods and constants for testing flight servers.
 */
public class FlightTestUtil {
  private static final Random RANDOM = new Random();

  public static final String LOCALHOST = "localhost";

  /**
   * Returns a a FlightServer (actually anything that is startable)
   * that has been started bound to a random port.
   */
  public static <T> T getStartedServer(Function<Integer, T> newServerFromPort) throws IOException {
    IOException lastThrown = null;
    T server = null;
    for (int x = 0; x < 3; x++) {
      final int port = 49152 + RANDOM.nextInt(5000);
      lastThrown = null;
      try {
        server = newServerFromPort.apply(port);
        try {
          server.getClass().getMethod("start").invoke(server);
        } catch (NoSuchMethodException | IllegalAccessException e) {
          throw new IllegalArgumentException("Couldn't call start method on object.", e);
        }
        break;
      } catch (InvocationTargetException e) {
        if (e.getTargetException() instanceof IOException) {
          lastThrown = (IOException) e.getTargetException();
        } else {
          throw (RuntimeException) e.getTargetException();
        }
      }
    }
    if (lastThrown != null) {
      throw lastThrown;
    }
    return server;
  }

  static boolean isEpollAvailable() {
    try {
      Class<?> epoll = Class.forName("io.netty.channel.epoll.Epoll");
      return (Boolean) epoll.getMethod("isAvailable").invoke(null);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return false;
    }
  }

  static boolean isKqueueAvailable() {
    try {
      Class<?> kqueue = Class.forName("io.netty.channel.kqueue.KQueue");
      return (Boolean) kqueue.getMethod("isAvailable").invoke(null);
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      return false;
    }
  }

  static boolean isNativeTransportAvailable() {
    return isEpollAvailable() || isKqueueAvailable();
  }

  private FlightTestUtil() {
  }
}

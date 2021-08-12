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

package org.apache.arrow.driver.jdbc.client.utils;

import java.util.Map.Entry;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Utility class for creating a client.
 */
public class ClientCreationUtils {
  private ClientCreationUtils() {
    // Prevent instantiation.
  }

  /**
   * Instantiates a new {@link FlightClient} from the provided info.
   *
   * @param address      the host and port for the connection to be established.
   * @param credentials  the username and password to use for authentication.
   * @param keyStoreInfo the keystore path and keystore password for TLS encryption.
   * @param allocator    the {@link BufferAllocator} to use.
   * @param options      the call options to use in subsequent calls to the client.
   * @return a new client.
   */
  public static FlightClient createNewClient(final Entry<String, Integer> address,
                                             final Entry<String, String> credentials,
                                             final Entry<String, String> keyStoreInfo,
                                             final BufferAllocator allocator,
                                             final CallOption... options) {
    // TODO
    return null;
  }
}

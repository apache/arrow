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

package org.apache.arrow.flight.client;

import java.util.function.Consumer;

import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.FlightConstants;
import org.apache.arrow.util.VisibleForTesting;

/**
 * Session ID from the server.
 */
public class ClientSessionWriter implements Consumer<CallHeaders> {
  private volatile String session;

  /**
   * Retrieves the current stored session.
   *
   * @return the current session.
   */
  @VisibleForTesting
  public String getSession() {
    return this.session;
  }

  /**
   * Sets the session.
   *
   * @param session the session from the server.
   */
  public void setSession(String session) {
    synchronized (this) {
      this.session = session;
    }
  }

  @Override
  public void accept(CallHeaders outgoingHeaders) {
    if (session != null) {
      outgoingHeaders.insert(FlightConstants.SESSION_HEADER, session);
    }
  }
}

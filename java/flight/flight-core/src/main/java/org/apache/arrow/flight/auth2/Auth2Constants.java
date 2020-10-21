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

package org.apache.arrow.flight.auth2;

/**
 * Constants used in authorization of flight connections.
 */
public final class Auth2Constants {
  public static final String PEER_IDENTITY_KEY = "arrow-flight-peer-identity";
  public static final String BEARER_PREFIX = "Bearer ";
  public static final String BASIC_PREFIX = "Basic ";
  public static final String AUTHORIZATION_HEADER = "Authorization";

  private Auth2Constants() {
  }
}

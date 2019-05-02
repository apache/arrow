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

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.arrow.flight.impl.Flight;

/** A URI where a Flight stream is available. */
public class Location {
  private final URI uri;

  /**
   * Constructs a new instance.
   *
   * @param uri the URI of the Flight service
   */
  public Location(String uri) throws URISyntaxException {
    super();
    this.uri = new URI(uri);
  }

  public Location(URI uri) {
    super();
    this.uri = uri;
  }

  public URI getUri() {
    return uri;
  }

  /**
   * Convert this Location into its protocol-level representation.
   */
  public Flight.Location toProtocol() {
    return Flight.Location.newBuilder().setUri(uri.toString()).build();
  }

  /** Construct a URI for a Flight+gRPC server without transport security. */
  public static Location forGrpcInsecure(String host, int port) {
    try {
      return new Location(new URI(LocationSchemes.GRPC_INSECURE, null, host, port, null, null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Construct a URI for a Flight+gRPC server with transport security. */
  public static Location forGrpcTls(String host, int port) {
    try {
      return new Location(new URI(LocationSchemes.GRPC_TLS, null, host, port, null, null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}

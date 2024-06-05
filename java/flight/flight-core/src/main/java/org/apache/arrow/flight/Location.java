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

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

import org.apache.arrow.flight.impl.Flight;

/** A URI where a Flight stream is available. */
public class Location {
  private final URI uri;

  /**
   * Constructs a new instance.
   *
   * @param uri the URI of the Flight service
   * @throws IllegalArgumentException if the URI scheme is unsupported
   */
  public Location(String uri) throws URISyntaxException {
    this(new URI(uri));
  }

  /**
   * Construct a new instance from an existing URI.
   *
   * @param uri the URI of the Flight service
   */
  public Location(URI uri) {
    super();
    Objects.requireNonNull(uri);
    this.uri = uri;
  }

  public URI getUri() {
    return uri;
  }

  /**
   * Helper method to turn this Location into a SocketAddress.
   *
   * @return null if could not be converted
   */
  public SocketAddress toSocketAddress() {
    switch (uri.getScheme()) {
      case LocationSchemes.GRPC:
      case LocationSchemes.GRPC_TLS:
      case LocationSchemes.GRPC_INSECURE: {
        return new InetSocketAddress(uri.getHost(), uri.getPort());
      }

      case LocationSchemes.GRPC_DOMAIN_SOCKET: {
        try {
          // This dependency is not available on non-Unix platforms.
          return Class.forName("io.netty.channel.unix.DomainSocketAddress").asSubclass(SocketAddress.class)
              .getConstructor(String.class)
              .newInstance(uri.getPath());
        } catch (InstantiationException | ClassNotFoundException | InvocationTargetException |
            NoSuchMethodException | IllegalAccessException e) {
          return null;
        }
      }

      default: {
        return null;
      }
    }
  }

  /**
   * Convert this Location into its protocol-level representation.
   */
  Flight.Location toProtocol() {
    return Flight.Location.newBuilder().setUri(uri.toString()).build();
  }

  /**
   * Construct a special URI to indicate to clients that they may fetch data by reusing
   * an existing connection to a Flight RPC server.
   */
  public static Location reuseConnection() {
    try {
      return new Location(new URI(LocationSchemes.REUSE_CONNECTION, "", "", "", null));
    } catch (URISyntaxException e) {
      // This should never happen.
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Construct a URI for a Flight+gRPC server without transport security.
   *
   * @throws IllegalArgumentException if the constructed URI is invalid.
   */
  public static Location forGrpcInsecure(String host, int port) {
    try {
      return new Location(new URI(LocationSchemes.GRPC_INSECURE, null, host, port, null, null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Construct a URI for a Flight+gRPC server with transport security.
   *
   * @throws IllegalArgumentException if the constructed URI is invalid.
   */
  public static Location forGrpcTls(String host, int port) {
    try {
      return new Location(new URI(LocationSchemes.GRPC_TLS, null, host, port, null, null, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Construct a URI for a Flight+gRPC server over a Unix domain socket.
   *
   * @throws IllegalArgumentException if the constructed URI is invalid.
   */
  public static Location forGrpcDomainSocket(String path) {
    try {
      return new Location(new URI(LocationSchemes.GRPC_DOMAIN_SOCKET, null, path, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public String toString() {
    return "Location{" +
        "uri=" + uri +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Location)) {
      return false;
    }
    Location location = (Location) o;
    return uri.equals(location.uri);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri);
  }
}

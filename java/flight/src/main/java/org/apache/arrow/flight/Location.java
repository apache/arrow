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
   * @throws IllegalArgumentException if the URI scheme is unsupported
   */
  public Location(URI uri) {
    super();
    this.uri = uri;
    // Validate the scheme
    switch (uri.getScheme()) {
      case LocationSchemes.GRPC:
      case LocationSchemes.GRPC_DOMAIN_SOCKET:
      case LocationSchemes.GRPC_INSECURE:
      case LocationSchemes.GRPC_TLS: {
        break;
      }
      default:
        throw new IllegalArgumentException("Scheme is not supported: " + this.uri);
    }
  }

  public URI getUri() {
    return uri;
  }

  /**
   * Helper method to turn this Location into a SocketAddress.
   *
   * @return null if could not be converted
   */
  SocketAddress toSocketAddress() {
    switch (uri.getScheme()) {
      case LocationSchemes.GRPC:
      case LocationSchemes.GRPC_TLS:
      case LocationSchemes.GRPC_INSECURE: {
        return new InetSocketAddress(uri.getHost(), uri.getPort());
      }

      case LocationSchemes.GRPC_DOMAIN_SOCKET: {
        try {
          // This dependency is not available on non-Unix platforms.
          return (SocketAddress) Class.forName("io.netty.channel.unix.DomainSocketAddress")
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

  /**
   * Construct a URI for a Flight+gRPC server over a Unix domain socket.
   */
  public static Location forGrpcDomainSocket(String path) {
    try {
      return new Location(new URI(LocationSchemes.GRPC_DOMAIN_SOCKET, null, path, null));
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }
}

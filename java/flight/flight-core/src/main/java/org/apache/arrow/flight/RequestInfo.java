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

import java.net.SocketAddress;

import io.grpc.Attributes;
import io.grpc.Grpc;
import io.grpc.ServerCall;

/** Information about a request. */
public final class RequestInfo {

  private final SocketAddress remoteAddress;
  private final SocketAddress localAddress;

  RequestInfo(final SocketAddress remoteAddress, final SocketAddress localAddress) {
    this.remoteAddress = remoteAddress;
    this.localAddress = localAddress;
  }

  RequestInfo() {
    this(null, null);
  }

  /**
   * Returns the SocketAddress associated with the client.
   *
   * @return SocketAddress
   */
  public SocketAddress getRemoteAddress() {
    return remoteAddress;
  }

  /**
   * Returns the SocketAddress associated with the server.
   *
   * @return SocketAddress
   */
  public SocketAddress getLocalAddress() {
    return localAddress;
  }

  /**
   * Creates a new RequestInfo from the information present on the ServerCall.
   *
   * @param call with the information
   * @return a RequestInfo with information extracted from ServerCall
   */
  public static <ReqT, RespT> RequestInfo fromCall(ServerCall<ReqT, RespT> call) {
    final Attributes attrs = call.getAttributes();
    if (attrs != null) {
      return new RequestInfo(
          attrs.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR), attrs.get(Grpc.TRANSPORT_ATTR_LOCAL_ADDR));
    }
    return new RequestInfo();
  }
}

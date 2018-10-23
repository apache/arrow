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

package org.apache.arrow.flight.auth;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

public class ClientAuthInterceptor implements ClientInterceptor {

  private volatile Metadata headerWithAuth;

  public void setToken(byte[] token) {
    final Metadata md = new Metadata();
    md.put(AuthConstants.TOKEN_KEY, token);
    this.headerWithAuth = md;
  }

  public ClientAuthInterceptor() {
  }

  public boolean hasToken() {
    return headerWithAuth != null;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
      CallOptions callOptions, Channel next) {
    ClientCall<ReqT, RespT> call = next.newCall(methodDescriptor, callOptions);

    // once we have an auth header, add that to the calls.
    if (headerWithAuth != null) {
      call = new HeaderAttachingClientCall<>(call);
    }

    return call;
  }

  private final class HeaderAttachingClientCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {

    private HeaderAttachingClientCall(ClientCall<ReqT, RespT> call) {
      super(call);
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      headers.merge(headerWithAuth);
      super.start(responseListener, headers);
    }
  }

}

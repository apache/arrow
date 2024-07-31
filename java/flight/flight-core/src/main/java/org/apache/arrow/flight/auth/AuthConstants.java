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

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.Metadata.BinaryMarshaller;
import io.grpc.MethodDescriptor;
import org.apache.arrow.flight.FlightConstants;

/** Constants used in authorization of flight connections. */
public final class AuthConstants {

  public static final String HANDSHAKE_DESCRIPTOR_NAME =
      MethodDescriptor.generateFullMethodName(FlightConstants.SERVICE, "Handshake");
  public static final String TOKEN_NAME = "Auth-Token-bin";
  public static final Metadata.Key<byte[]> TOKEN_KEY =
      Metadata.Key.of(
          TOKEN_NAME,
          new BinaryMarshaller<byte[]>() {

            @Override
            public byte[] toBytes(byte[] value) {
              return value;
            }

            @Override
            public byte[] parseBytes(byte[] serialized) {
              return serialized;
            }
          });

  public static final Context.Key<String> PEER_IDENTITY_KEY =
      Context.keyWithDefault("arrow-flight-peer-identity", "");

  private AuthConstants() {}
}

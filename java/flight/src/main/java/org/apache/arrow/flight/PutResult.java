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

import java.nio.ByteBuffer;

import org.apache.arrow.flight.impl.Flight;

import com.google.protobuf.ByteString;

/**
 * A message from the server during a DoPut operation.
 */
public class PutResult {

  private ByteBuffer applicationMetadata;

  private PutResult(ByteBuffer metadata) {
    applicationMetadata = metadata;
  }

  /** Create a PutResult with application-specific metadata. */
  public static PutResult metadata(byte[] metadata) {
    if (metadata == null) {
      return empty();
    }
    return new PutResult(ByteBuffer.wrap(metadata));
  }

  /** Create an empty PutResult. */
  public static PutResult empty() {
    return new PutResult(null);
  }

  /** Get the metadata in this message. May be null. */
  public ByteBuffer getApplicationMetadata() {
    return applicationMetadata;
  }

  Flight.PutResult toProtocol() {
    if (applicationMetadata == null) {
      return Flight.PutResult.getDefaultInstance();
    }
    return Flight.PutResult.newBuilder().setAppMetadata(ByteString.copyFrom(applicationMetadata)).build();
  }

  static PutResult fromProtocol(Flight.PutResult message) {
    return new PutResult(message.getAppMetadata().asReadOnlyByteBuffer());
  }
}

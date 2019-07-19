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

import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.ReferenceManager;

import com.google.protobuf.ByteString;

import io.netty.buffer.ArrowBuf;

/**
 * A message from the server during a DoPut operation.
 *
 * <p>This object owns an {@link ArrowBuf} and should be closed when you are done with it.
 */
public class PutResult implements AutoCloseable {

  private ArrowBuf applicationMetadata;

  private PutResult(ArrowBuf metadata) {
    applicationMetadata = metadata;
  }

  /**
   * Create a PutResult with application-specific metadata.
   *
   * <p>This method assumes ownership of the {@link ArrowBuf}.
   */
  public static PutResult metadata(ArrowBuf metadata) {
    if (metadata == null) {
      return empty();
    }
    return new PutResult(metadata);
  }

  /** Create an empty PutResult. */
  public static PutResult empty() {
    return new PutResult(null);
  }

  /**
   * Get the metadata in this message. May be null.
   *
   * <p>Ownership of the {@link ArrowBuf} is retained by this object. Call {@link ReferenceManager#retain()} to preserve
   * a reference.
   */
  public ArrowBuf getApplicationMetadata() {
    return applicationMetadata;
  }

  Flight.PutResult toProtocol() {
    if (applicationMetadata == null) {
      return Flight.PutResult.getDefaultInstance();
    }
    return Flight.PutResult.newBuilder().setAppMetadata(ByteString.copyFrom(applicationMetadata.nioBuffer())).build();
  }

  /**
   * Construct a PutResult from a Protobuf message.
   *
   * @param allocator The allocator to use for allocating application metadata memory. The result object owns the
   *     allocated buffer, if any.
   * @param message The gRPC/Protobuf message.
   */
  static PutResult fromProtocol(BufferAllocator allocator, Flight.PutResult message) {
    final ArrowBuf buf = allocator.buffer(message.getAppMetadata().size());
    message.getAppMetadata().asReadOnlyByteBufferList().forEach(bb -> {
      buf.setBytes(buf.writerIndex(), bb);
      buf.writerIndex(buf.writerIndex() + bb.limit());
    });
    return new PutResult(buf);
  }

  @Override
  public void close() {
    if (applicationMetadata != null) {
      applicationMetadata.close();
    }
  }
}

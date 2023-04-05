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

import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

/**
 * Method option for supplying headers to method calls.
 */
public class HeaderCallOption implements CallOptions.GrpcCallOption {
  private final Metadata propertiesMetadata = new Metadata();

  /**
   * Header property constructor.
   *
   * @param headers the headers that should be sent across. If a header is a string, it should only be valid ASCII
   *                characters. Binary headers should end in "-bin".
   */
  public HeaderCallOption(CallHeaders headers) {
    for (String key : headers.keys()) {
      if (key.endsWith(Metadata.BINARY_HEADER_SUFFIX)) {
        final Metadata.Key<byte[]> metaKey = Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER);
        headers.getAllByte(key).forEach(v -> propertiesMetadata.put(metaKey, v));
      } else {
        final Metadata.Key<String> metaKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
        headers.getAll(key).forEach(v -> propertiesMetadata.put(metaKey, v));
      }
    }
  }

  @Override
  public <T extends AbstractStub<T>> T wrapStub(T stub) {
    return stub.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(propertiesMetadata));
  }
}

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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import io.grpc.Metadata;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.MetadataUtils;

/**
 * Method option for supplying properties to method calls.
 */
public class PropertyCallOption implements CallOptions.GrpcCallOption {
  private final Metadata propertiesMetadata;

  /**
   * Single property constructor.
   */
  public PropertyCallOption(String key, String value) {
    this(Collections.singletonMap(key, value));
  }

  /**
   * Multi-property constructor.
   */
  public PropertyCallOption(Map<String, Serializable> properties) {
    // Encode the properties as a set of key/value Base64 encoded strings.
    final StringBuilder value = new StringBuilder();
    final Base64.Encoder encoder = Base64.getEncoder().withoutPadding();
    for (Map.Entry<String, Serializable> property : properties.entrySet()) {
      value.append(encoder.encodeToString(property.getKey().getBytes()));
      value.append("=");
      try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
          ObjectOutputStream outStream = new ObjectOutputStream(byteStream)) {
        outStream.writeObject(property.getValue());
        value.append(encoder.encodeToString(byteStream.toByteArray()));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      value.append(";");
    }

    propertiesMetadata = new Metadata();
    propertiesMetadata.put(
        Metadata.Key.of(FlightConstants.PROPERTY_HEADER, Metadata.ASCII_STRING_MARSHALLER),
        value.toString());
  }

  @Override
  public <T extends AbstractStub<T>> T wrapStub(T stub) {
    return MetadataUtils.attachHeaders(stub, propertiesMetadata);
  }
}

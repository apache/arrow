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

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Objects;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.validate.MetadataV4UnionChecker;

/**
 * Opaque result returned after executing a getSchema request.
 *
 * <p>POJO wrapper around the Flight protocol buffer message sharing the same name.
 */
public class SchemaResult {

  private final Schema schema;
  private final IpcOption option;

  public SchemaResult(Schema schema) {
    this(schema, IpcOption.DEFAULT);
  }

  /** Create a schema result with specific IPC options for serialization. */
  public SchemaResult(Schema schema, IpcOption option) {
    Objects.requireNonNull(schema);
    MetadataV4UnionChecker.checkForUnion(schema.getFields().iterator(), option.metadataVersion);
    this.schema = schema;
    this.option = option;
  }

  public Schema getSchema() {
    return schema;
  }

  /** Converts to the protocol buffer representation. */
  Flight.SchemaResult toProtocol() {
    // Encode schema in a Message payload
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      MessageSerializer.serialize(new WriteChannel(Channels.newChannel(baos)), schema, option);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Flight.SchemaResult.newBuilder()
        .setSchema(ByteString.copyFrom(baos.toByteArray()))
        .build();
  }

  /** Converts from the protocol buffer representation. */
  static SchemaResult fromProtocol(Flight.SchemaResult pbSchemaResult) {
    try {
      final ByteBuffer schemaBuf = pbSchemaResult.getSchema().asReadOnlyByteBuffer();
      Schema schema =
          pbSchemaResult.getSchema().size() > 0
              ? MessageSerializer.deserializeSchema(
                  new ReadChannel(Channels.newChannel(new ByteBufferBackedInputStream(schemaBuf))))
              : new Schema(ImmutableList.of());
      return new SchemaResult(schema);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

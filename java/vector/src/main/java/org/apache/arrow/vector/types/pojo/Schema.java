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

package org.apache.arrow.vector.types.pojo;


import static org.apache.arrow.vector.types.pojo.Field.convertField;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.flatbuf.Endianness;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.FBSerializables;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.flatbuffers.FlatBufferBuilder;

/**
 * An Arrow Schema.
 */
public class Schema {

  /**
   * Search for a field by name in given the list of fields.
   *
   * @param fields the list of the fields
   * @param name   the name of the field to return
   * @return the corresponding field
   * @throws IllegalArgumentException if the field was not found
   */
  public static Field findField(List<Field> fields, String name) {
    for (Field field : fields) {
      if (field.getName().equals(name)) {
        return field;
      }
    }
    throw new IllegalArgumentException(String.format("field %s not found in %s", name, fields));
  }

  static final String METADATA_KEY = "key";
  static final String METADATA_VALUE = "value";

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
  private static final ObjectReader reader = mapper.readerFor(Schema.class);
  private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  public static Schema fromJSON(String json) throws IOException {
    return reader.readValue(Preconditions.checkNotNull(json));
  }

  /**
   * Deserialize a schema that has been serialized using {@link #toByteArray()}.
   * @param buffer the bytes to deserialize.
   * @return The deserialized schema.
   */
  @Deprecated
  public static Schema deserialize(ByteBuffer buffer) {
    return convertSchema(org.apache.arrow.flatbuf.Schema.getRootAsSchema(buffer));
  }

  /**
   * Deserialize a schema that has been serialized as a message using {@link #serializeAsMessage()}.
   * @param buffer the bytes to deserialize.
   * @return The deserialized schema.
   */
  public static Schema deserializeMessage(ByteBuffer buffer) {
    ByteBufferBackedInputStream stream = new ByteBufferBackedInputStream(buffer);
    try (ReadChannel channel = new ReadChannel(Channels.newChannel(stream))) {
      return MessageSerializer.deserializeSchema(channel);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** Converts a flatbuffer schema to its POJO representation. */
  public static Schema convertSchema(org.apache.arrow.flatbuf.Schema schema) {
    List<Field> fields = new ArrayList<>();
    for (int i = 0; i < schema.fieldsLength(); i++) {
      fields.add(convertField(schema.fields(i)));
    }
    Map<String, String> metadata = new HashMap<>();
    for (int i = 0; i < schema.customMetadataLength(); i++) {
      KeyValue kv = schema.customMetadata(i);
      String key = kv.key();
      String value = kv.value();
      metadata.put(key == null ? "" : key, value == null ? "" : value);
    }
    return new Schema(true, Collections.unmodifiableList(fields), Collections.unmodifiableMap(metadata));
  }

  private final List<Field> fields;
  private final Map<String, String> metadata;

  public Schema(Iterable<Field> fields) {
    this(fields, (Map<String, String>) null);
  }

  /**
   * Constructor with metadata.
   */
  public Schema(Iterable<Field> fields,
                Map<String, String> metadata) {
    this(true,
        Collections2.toImmutableList(fields),
        metadata == null ? Collections.emptyMap() : Collections2.immutableMapCopy(metadata));
  }


  /**
   * Constructor used for JSON deserialization.
   */
  @JsonCreator
  private Schema(@JsonProperty("fields") Iterable<Field> fields,
                @JsonProperty("metadata") List<Map<String, String>> metadata) {
    this(fields, convertMetadata(metadata));
  }


  /**
   * Private constructor to bypass automatic collection copy.
   * @param ignored an ignored argument. Its only purpose is to prevent using the constructor
   *     by accident because of type collisions (List vs Iterable).
   */
  private Schema(boolean ignored, List<Field> fields, Map<String, String> metadata) {
    this.fields = fields;
    this.metadata = metadata;
  }

  static Map<String, String> convertMetadata(List<Map<String, String>> metadata) {
    return (metadata == null) ? null : metadata.stream()
        .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.get(METADATA_KEY), e.get(METADATA_VALUE)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static List<Map<String, String>> convertMetadata(Map<String, String> metadata) {
    return (metadata == null) ? null : metadata.entrySet()
        .stream()
        .map(Schema::convertEntryToKeyValueMap)
        .collect(Collectors.toList());
  }

  private static Map<String, String> convertEntryToKeyValueMap(Map.Entry<String, String> entry) {
    Map<String, String> map = new HashMap<>(2);
    map.put(METADATA_KEY, entry.getKey());
    map.put(METADATA_VALUE, entry.getValue());
    return Collections.unmodifiableMap(map);
  }

  public List<Field> getFields() {
    return fields;
  }

  @JsonIgnore
  public Map<String, String> getCustomMetadata() {
    return metadata;
  }

  @JsonProperty("metadata")
  @JsonInclude(Include.NON_EMPTY)
  List<Map<String, String>> getCustomMetadataForJson() {
    return convertMetadata(getCustomMetadata());
  }

  /**
   * Search for a field by name in this Schema.
   *
   * @param name the name of the field to return
   * @return the corresponding field
   * @throws IllegalArgumentException if the field was not found
   */
  public Field findField(String name) {
    return findField(getFields(), name);
  }

  /**
   * Returns the JSON string representation of this schema.
   */
  public String toJson() {
    try {
      return writer.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      // this should not happen
      throw new RuntimeException(e);
    }
  }

  /**
   *  Adds this schema to the builder returning the size of the builder after adding.
   */
  public int getSchema(FlatBufferBuilder builder) {
    int[] fieldOffsets = new int[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      fieldOffsets[i] = fields.get(i).getField(builder);
    }
    int fieldsOffset = org.apache.arrow.flatbuf.Schema.createFieldsVector(builder, fieldOffsets);
    int metadataOffset = FBSerializables.writeKeyValues(builder, metadata);
    org.apache.arrow.flatbuf.Schema.startSchema(builder);
    org.apache.arrow.flatbuf.Schema.addEndianness(builder,
        (LITTLE_ENDIAN ? Endianness.Little : Endianness.Big));
    org.apache.arrow.flatbuf.Schema.addFields(builder, fieldsOffset);
    org.apache.arrow.flatbuf.Schema.addCustomMetadata(builder, metadataOffset);
    return org.apache.arrow.flatbuf.Schema.endSchema(builder);
  }

  /**
   * Returns the serialized flatbuffer bytes of the schema wrapped in a message table.
   * Use {@link #deserializeMessage(ByteBuffer)} to rebuild the Schema.
   */
  public byte[] serializeAsMessage() {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (WriteChannel channel = new WriteChannel(Channels.newChannel(out))) {
      MessageSerializer.serialize(channel, this);
      return out.toByteArray();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * Returns the serialized flatbuffer representation of this schema.
   * @deprecated This method does not encapsulate the schema in a Message payload which is incompatible with other
   *     languages. Use {@link #serializeAsMessage()} instead.
   */
  @Deprecated
  public byte[] toByteArray() {
    FlatBufferBuilder builder = new FlatBufferBuilder();
    int schemaOffset = this.getSchema(builder);
    builder.finish(schemaOffset);
    ByteBuffer bb = builder.dataBuffer();
    byte[] bytes = new byte[bb.remaining()];
    bb.get(bytes);
    return bytes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, metadata);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Schema)) {
      return false;
    }
    return Objects.equals(this.fields, ((Schema) obj).fields) &&
        Objects.equals(this.metadata, ((Schema) obj).metadata);
  }

  @Override
  public String toString() {
    String meta = metadata.isEmpty() ? "" : "(metadata: " + metadata.toString() + ")";
    return "Schema<" + fields.stream().map(t -> t.toString()).collect(Collectors.joining(", ")) + ">" + meta;
  }
}

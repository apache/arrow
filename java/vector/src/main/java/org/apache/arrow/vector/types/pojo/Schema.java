/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.vector.types.pojo;


import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.arrow.vector.types.pojo.Field.convertField;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.flatbuffers.FlatBufferBuilder;

import org.apache.arrow.flatbuf.KeyValue;

/**
 * An Arrow Schema
 */
public class Schema {

  /**
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

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
  private static final ObjectReader reader = mapper.readerFor(Schema.class);

  public static Schema fromJSON(String json) throws IOException {
    return reader.readValue(checkNotNull(json));
  }

  public static Schema deserialize(ByteBuffer buffer) {
    return convertSchema(org.apache.arrow.flatbuf.Schema.getRootAsSchema(buffer));
  }

  public static Schema convertSchema(org.apache.arrow.flatbuf.Schema schema) {
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    for (int i = 0; i < schema.fieldsLength(); i++) {
      childrenBuilder.add(convertField(schema.fields(i)));
    }
    List<Field> fields = childrenBuilder.build();
    ImmutableMap.Builder<String, String> metadataBuilder = ImmutableMap.builder();
    for (int i = 0; i < schema.customMetadataLength(); i++) {
      KeyValue kv = schema.customMetadata(i);
      String key = kv.key(), value = kv.value();
      metadataBuilder.put(key == null ? "" : key, value == null ? "" : value);
    }
    Map<String, String> metadata = metadataBuilder.build();
    return new Schema(fields, metadata);
  }

  private final List<Field> fields;
  private final Map<String, String> metadata;

  public Schema(Iterable<Field> fields) {
    this(fields, null);
  }

  @JsonCreator
  public Schema(@JsonProperty("fields") Iterable<Field> fields,
                @JsonProperty("metadata") Map<String, String> metadata) {
    List<Field> fieldList = new ArrayList<>();
    for (Field field : fields) {
      fieldList.add(field);
    }
    this.fields = Collections.unmodifiableList(fieldList);
    this.metadata = metadata == null ? ImmutableMap.<String, String>of() : ImmutableMap.copyOf(metadata);
  }

  public List<Field> getFields() {
    return fields;
  }

  @JsonInclude(Include.NON_EMPTY)
  public Map<String, String> getCustomMetadata() {
    return metadata;
  }

  /**
   * @param name the name of the field to return
   * @return the corresponding field
   */
  public Field findField(String name) {
    return findField(getFields(), name);
  }

  public String toJson() {
    try {
      return writer.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      // this should not happen
      throw new RuntimeException(e);
    }
  }

  public int getSchema(FlatBufferBuilder builder) {
    int[] fieldOffsets = new int[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      fieldOffsets[i] = fields.get(i).getField(builder);
    }
    int fieldsOffset = org.apache.arrow.flatbuf.Schema.createFieldsVector(builder, fieldOffsets);
    int[] metadataOffsets = new int[metadata.size()];
    Iterator<Entry<String, String>> metadataIterator = metadata.entrySet().iterator();
    for (int i = 0; i < metadataOffsets.length; i++) {
      Entry<String, String> kv = metadataIterator.next();
      int keyOffset = builder.createString(kv.getKey());
      int valueOffset = builder.createString(kv.getValue());
      KeyValue.startKeyValue(builder);
      KeyValue.addKey(builder, keyOffset);
      KeyValue.addValue(builder, valueOffset);
      metadataOffsets[i] = KeyValue.endKeyValue(builder);
    }
    int metadataOffset = org.apache.arrow.flatbuf.Field.createCustomMetadataVector(builder, metadataOffsets);
    org.apache.arrow.flatbuf.Schema.startSchema(builder);
    org.apache.arrow.flatbuf.Schema.addFields(builder, fieldsOffset);
    org.apache.arrow.flatbuf.Schema.addCustomMetadata(builder, metadataOffset);
    return org.apache.arrow.flatbuf.Schema.endSchema(builder);
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
    return "Schema<" + Joiner.on(", ").join(fields) + ">" + meta;
  }
}

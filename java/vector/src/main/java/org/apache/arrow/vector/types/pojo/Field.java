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

import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.vector.complex.BaseRepeatedValueVector.DATA_VECTOR_NAME;
import static org.apache.arrow.vector.types.pojo.ArrowType.getTypeForField;
import static org.apache.arrow.vector.types.pojo.Schema.convertMetadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.flatbuffers.FlatBufferBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Type;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A POJO abstraction for the Flatbuffer description of Vector Type. */
public class Field {

  private static final Logger logger = LoggerFactory.getLogger(Field.class);

  public static Field nullablePrimitive(String name, ArrowType.PrimitiveType type) {
    return nullable(name, type);
  }

  public static Field nullable(String name, ArrowType type) {
    return new Field(name, FieldType.nullable(type), null);
  }

  public static Field notNullable(String name, ArrowType type) {
    return new Field(name, FieldType.notNullable(type), null);
  }

  private final String name;
  private final FieldType fieldType;
  private final List<Field> children;

  private Field(
      String name,
      boolean nullable,
      ArrowType type,
      DictionaryEncoding dictionary,
      List<Field> children,
      Map<String, String> metadata) {
    this(name, new FieldType(nullable, type, dictionary, metadata), children);
  }

  @JsonCreator
  private Field(
      @JsonProperty("name") String name,
      @JsonProperty("nullable") boolean nullable,
      @JsonProperty("type") ArrowType type,
      @JsonProperty("dictionary") DictionaryEncoding dictionary,
      @JsonProperty("children") List<Field> children,
      @JsonProperty("metadata") List<Map<String, String>> metadata) {
    this(name, new FieldType(nullable, type, dictionary, convertMetadata(metadata)), children);
  }

  /**
   * Constructs a new Field object.
   *
   * @param name name of the field
   * @param fieldType type of the field
   * @param children child fields, if any
   */
  public Field(String name, FieldType fieldType, List<Field> children) {
    this.name = name;
    this.fieldType = checkNotNull(fieldType);
    this.children =
        children == null ? Collections.emptyList() : Collections2.toImmutableList(children);
  }

  /** Construct a new vector of this type using the given allocator. */
  public FieldVector createVector(BufferAllocator allocator) {
    FieldVector vector = fieldType.createNewSingleVector(this, allocator, null);
    vector.initializeChildrenFromFields(children);
    return vector;
  }

  /** Constructs a new instance from a flatbuffer representation of the field. */
  public static Field convertField(org.apache.arrow.flatbuf.Field field) {
    Map<String, String> metadata = new HashMap<>();
    for (int i = 0; i < field.customMetadataLength(); i++) {
      KeyValue kv = field.customMetadata(i);
      String key = kv.key();
      String value = kv.value();
      metadata.put(key == null ? "" : key, value == null ? "" : value);
    }
    metadata = Collections.unmodifiableMap(metadata);

    String name = field.name();
    boolean nullable = field.nullable();
    ArrowType type = getTypeForField(field);

    if (metadata.containsKey(ExtensionType.EXTENSION_METADATA_KEY_NAME)) {
      final String extensionName = metadata.get(ExtensionType.EXTENSION_METADATA_KEY_NAME);
      final String extensionMetadata =
          metadata.getOrDefault(ExtensionType.EXTENSION_METADATA_KEY_METADATA, "");
      ExtensionType extensionType = ExtensionTypeRegistry.lookup(extensionName);
      if (extensionType != null) {
        type = extensionType.deserialize(type, extensionMetadata);
      } else {
        // Otherwise, we haven't registered the type
        logger.info("Unrecognized extension type: {}", extensionName);
      }
    }

    DictionaryEncoding dictionary = null;
    org.apache.arrow.flatbuf.DictionaryEncoding dictionaryFB = field.dictionary();
    if (dictionaryFB != null) {
      ArrowType.Int indexType = null;
      org.apache.arrow.flatbuf.Int indexTypeFB = dictionaryFB.indexType();
      if (indexTypeFB != null) {
        indexType = new ArrowType.Int(indexTypeFB.bitWidth(), indexTypeFB.isSigned());
      }
      dictionary = new DictionaryEncoding(dictionaryFB.id(), dictionaryFB.isOrdered(), indexType);
    }
    List<Field> children = new ArrayList<>();
    for (int i = 0; i < field.childrenLength(); i++) {
      Field childField = convertField(field.children(i));
      childField = mutateOriginalNameIfNeeded(field, childField);
      children.add(childField);
    }
    children = Collections.unmodifiableList(children);
    return new Field(name, nullable, type, dictionary, children, metadata);
  }

  /**
   * Helper method to ensure backward compatibility with schemas generated prior to ARROW-1347,
   * ARROW-1663.
   *
   * @param field the field to check
   * @param originalChildField original field which name might be mutated
   * @return original or mutated field
   */
  private static Field mutateOriginalNameIfNeeded(
      org.apache.arrow.flatbuf.Field field, Field originalChildField) {
    if ((field.typeType() == Type.List || field.typeType() == Type.FixedSizeList)
        && originalChildField.getName().equals("[DEFAULT]")) {
      return new Field(
          DATA_VECTOR_NAME,
          originalChildField.isNullable(),
          originalChildField.getType(),
          originalChildField.getDictionary(),
          originalChildField.getChildren(),
          originalChildField.getMetadata());
    }
    return originalChildField;
  }

  /**
   * Puts this object into <code>builder</code> and returns the length of the serialized flatbuffer.
   */
  public int getField(FlatBufferBuilder builder) {
    int nameOffset = name == null ? -1 : builder.createString(name);
    int typeOffset = getType().getType(builder);
    int dictionaryOffset = -1;
    DictionaryEncoding dictionary = getDictionary();
    if (dictionary != null) {
      int dictionaryType = dictionary.getIndexType().getType(builder);
      org.apache.arrow.flatbuf.DictionaryEncoding.startDictionaryEncoding(builder);
      org.apache.arrow.flatbuf.DictionaryEncoding.addId(builder, dictionary.getId());
      org.apache.arrow.flatbuf.DictionaryEncoding.addIsOrdered(builder, dictionary.isOrdered());
      org.apache.arrow.flatbuf.DictionaryEncoding.addIndexType(builder, dictionaryType);
      dictionaryOffset = org.apache.arrow.flatbuf.DictionaryEncoding.endDictionaryEncoding(builder);
    }
    int[] childrenData = new int[children.size()];
    for (int i = 0; i < children.size(); i++) {
      childrenData[i] = children.get(i).getField(builder);
    }
    int childrenOffset = org.apache.arrow.flatbuf.Field.createChildrenVector(builder, childrenData);
    int[] metadataOffsets = new int[getMetadata().size()];
    Iterator<Entry<String, String>> metadataIterator = getMetadata().entrySet().iterator();
    for (int i = 0; i < metadataOffsets.length; i++) {
      Entry<String, String> kv = metadataIterator.next();
      int keyOffset = builder.createString(kv.getKey());
      int valueOffset = builder.createString(kv.getValue());
      KeyValue.startKeyValue(builder);
      KeyValue.addKey(builder, keyOffset);
      KeyValue.addValue(builder, valueOffset);
      metadataOffsets[i] = KeyValue.endKeyValue(builder);
    }
    int metadataOffset =
        org.apache.arrow.flatbuf.Field.createCustomMetadataVector(builder, metadataOffsets);
    org.apache.arrow.flatbuf.Field.startField(builder);
    if (name != null) {
      org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    }
    org.apache.arrow.flatbuf.Field.addNullable(builder, isNullable());
    org.apache.arrow.flatbuf.Field.addTypeType(builder, getType().getTypeID().getFlatbufID());
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    org.apache.arrow.flatbuf.Field.addCustomMetadata(builder, metadataOffset);
    if (dictionary != null) {
      org.apache.arrow.flatbuf.Field.addDictionary(builder, dictionaryOffset);
    }
    return org.apache.arrow.flatbuf.Field.endField(builder);
  }

  public String getName() {
    return name;
  }

  public boolean isNullable() {
    return fieldType.isNullable();
  }

  public ArrowType getType() {
    return fieldType.getType();
  }

  @JsonIgnore
  public FieldType getFieldType() {
    return fieldType;
  }

  @JsonInclude(Include.NON_NULL)
  public DictionaryEncoding getDictionary() {
    return fieldType.getDictionary();
  }

  public List<Field> getChildren() {
    return children;
  }

  @JsonIgnore
  public Map<String, String> getMetadata() {
    return fieldType.getMetadata();
  }

  @JsonProperty("metadata")
  @JsonInclude(Include.NON_EMPTY)
  List<Map<String, String>> getMetadataForJson() {
    return convertMetadata(getMetadata());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isNullable(), getType(), getDictionary(), getMetadata(), children);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Field)) {
      return false;
    }
    Field that = (Field) obj;
    return Objects.equals(this.name, that.name)
        && this.isNullable() == that.isNullable()
        && Objects.equals(this.getType(), that.getType())
        && Objects.equals(this.getDictionary(), that.getDictionary())
        && Objects.equals(this.getMetadata(), that.getMetadata())
        && Objects.equals(this.children, that.children);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (name != null) {
      sb.append(name).append(": ");
    }
    sb.append(getType());
    if (getDictionary() != null) {
      sb.append("[dictionary: ").append(getDictionary().getId()).append("]");
    }
    if (!children.isEmpty()) {
      sb.append("<")
          .append(children.stream().map(t -> t.toString()).collect(Collectors.joining(", ")))
          .append(">");
    }
    if (!isNullable()) {
      sb.append(" not null");
    }
    return sb.toString();
  }
}

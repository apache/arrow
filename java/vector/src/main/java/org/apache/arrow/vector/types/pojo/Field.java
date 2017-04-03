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
import static org.apache.arrow.vector.types.pojo.ArrowType.getTypeForField;

import java.util.List;
import java.util.Objects;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.schema.TypeLayout;
import org.apache.arrow.vector.schema.VectorLayout;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

public class Field {

  public static Field nullablePrimitive(String name, ArrowType.PrimitiveType type) {
    return nullable(name, type);
  }

  public static Field nullable(String name, ArrowType type) {
    return new Field(name, true, type, null, null);
  }

  private final String name;
  private final FieldType fieldType;
  private final List<Field> children;
  private final TypeLayout typeLayout;

  @JsonCreator
  private Field(
      @JsonProperty("name") String name,
      @JsonProperty("nullable") boolean nullable,
      @JsonProperty("type") ArrowType type,
      @JsonProperty("dictionary") DictionaryEncoding dictionary,
      @JsonProperty("children") List<Field> children,
      @JsonProperty("typeLayout") TypeLayout typeLayout) {
    this(name, new FieldType(nullable, type, dictionary), children, typeLayout);
  }

  private Field(String name, FieldType fieldType, List<Field> children, TypeLayout typeLayout) {
    super();
    this.name = name;
    this.fieldType = checkNotNull(fieldType);
    this.children = children == null ? ImmutableList.<Field>of() : children;
    this.typeLayout = checkNotNull(typeLayout);
  }

  public Field(String name, FieldType fieldType, List<Field> children) {
    this(name, fieldType, children, TypeLayout.getTypeLayout(fieldType.getType()));
  }

  public Field(String name, boolean nullable, ArrowType type, List<Field> children) {
    this(name, nullable, type, null, children);
  }

  public Field(String name, boolean nullable, ArrowType type, DictionaryEncoding dictionary, List<Field> children) {
    this(name, new FieldType(nullable, type, dictionary), children);
  }

  public FieldVector createVector(BufferAllocator allocator) {
    FieldVector vector = fieldType.createNewSingleVector(name, allocator, null);
    vector.initializeChildrenFromFields(children);
    return vector;
  }

  public static Field convertField(org.apache.arrow.flatbuf.Field field) {
    String name = field.name();
    boolean nullable = field.nullable();
    ArrowType type = getTypeForField(field);
    DictionaryEncoding dictionary = null;
    org.apache.arrow.flatbuf.DictionaryEncoding dictionaryFB = field.dictionary();
    if (dictionaryFB != null) {
      Int indexType = null;
      org.apache.arrow.flatbuf.Int indexTypeFB = dictionaryFB.indexType();
      if (indexTypeFB != null) {
        indexType = new Int(indexTypeFB.bitWidth(), indexTypeFB.isSigned());
      }
      dictionary = new DictionaryEncoding(dictionaryFB.id(), dictionaryFB.isOrdered(), indexType);
    }
    ImmutableList.Builder<org.apache.arrow.vector.schema.VectorLayout> layout = ImmutableList.builder();
    for (int i = 0; i < field.layoutLength(); ++i) {
      layout.add(new org.apache.arrow.vector.schema.VectorLayout(field.layout(i)));
    }
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    for (int i = 0; i < field.childrenLength(); i++) {
      childrenBuilder.add(convertField(field.children(i)));
    }
    List<Field> children = childrenBuilder.build();
    return new Field(name, nullable, type, dictionary, children, new TypeLayout(layout.build()));
  }

  public void validate() {
    TypeLayout expectedLayout = TypeLayout.getTypeLayout(getType());
    if (!expectedLayout.equals(typeLayout)) {
      throw new IllegalArgumentException("Deserialized field does not match expected vectors. expected: " + expectedLayout + " got " + typeLayout);
    }
  }

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
    int[] buffersData = new int[typeLayout.getVectors().size()];
    for (int i = 0; i < buffersData.length; i++) {
      VectorLayout vectorLayout = typeLayout.getVectors().get(i);
      buffersData[i] = vectorLayout.writeTo(builder);
    }
    int layoutOffset =  org.apache.arrow.flatbuf.Field.createLayoutVector(builder, buffersData);
    org.apache.arrow.flatbuf.Field.startField(builder);
    if (name != null) {
      org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    }
    org.apache.arrow.flatbuf.Field.addNullable(builder, isNullable());
    org.apache.arrow.flatbuf.Field.addTypeType(builder, getType().getTypeID().getFlatbufID());
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    org.apache.arrow.flatbuf.Field.addLayout(builder, layoutOffset);
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

  public TypeLayout getTypeLayout() {
    return typeLayout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, isNullable(), getType(), getDictionary(), children);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Field)) {
      return false;
    }
    Field that = (Field) obj;
    return Objects.equals(this.name, that.name) &&
            Objects.equals(this.isNullable(), that.isNullable()) &&
            Objects.equals(this.getType(), that.getType()) &&
           Objects.equals(this.getDictionary(), that.getDictionary()) &&
            Objects.equals(this.children, that.children);
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
      sb.append("<").append(Joiner.on(", ").join(children)).append(">");
    }
    if (!isNullable()) {
      sb.append(" not null");
    }
    return sb.toString();
  }
}

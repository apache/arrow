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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.arrow.flatbuf.DictionaryEncoding;
import org.apache.arrow.vector.schema.TypeLayout;
import org.apache.arrow.vector.schema.VectorLayout;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

public class Field {
  private final String name;
  private final boolean nullable;
  private final ArrowType type;
  private final Long dictionary;
  private final List<Field> children;
  private final TypeLayout typeLayout;

  @JsonCreator
  private Field(
      @JsonProperty("name") String name,
      @JsonProperty("nullable") boolean nullable,
      @JsonProperty("type") ArrowType type,
      @JsonProperty("dictionary") Long dictionary,
      @JsonProperty("children") List<Field> children,
      @JsonProperty("typeLayout") TypeLayout typeLayout) {
    this.name = name;
    this.nullable = nullable;
    this.type = checkNotNull(type);
    this.dictionary = dictionary;
    if (children == null) {
      this.children = ImmutableList.of();
    } else {
      this.children = children;
    }
    this.typeLayout = checkNotNull(typeLayout);
  }

  public Field(String name, boolean nullable, ArrowType type, List<Field> children) {
    this(name, nullable, type, null, children, TypeLayout.getTypeLayout(checkNotNull(type)));
  }

  public Field(String name, boolean nullable, ArrowType type, Long dictionary, List<Field> children) {
    this(name, nullable, type, dictionary, children, TypeLayout.getTypeLayout(checkNotNull(type)));
  }

  public static Field convertField(org.apache.arrow.flatbuf.Field field) {
    String name = field.name();
    boolean nullable = field.nullable();
    ArrowType type = getTypeForField(field);
    DictionaryEncoding dictionaryEncoding = field.dictionary();
    Long dictionary = null;
    if (dictionaryEncoding != null) {
      dictionary = dictionaryEncoding.id();
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
    TypeLayout expectedLayout = TypeLayout.getTypeLayout(type);
    if (!expectedLayout.equals(typeLayout)) {
      throw new IllegalArgumentException("Deserialized field does not match expected vectors. expected: " + expectedLayout + " got " + typeLayout);
    }
  }

  public int getField(FlatBufferBuilder builder) {
    int nameOffset = name == null ? -1 : builder.createString(name);
    int typeOffset = type.getType(builder);
    int dictionaryOffset = -1;
    if (dictionary != null) {
      builder.addLong(dictionary);
      dictionaryOffset = builder.offset();
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
    org.apache.arrow.flatbuf.Field.addNullable(builder, nullable);
    org.apache.arrow.flatbuf.Field.addTypeType(builder, type.getTypeID().getFlatbufID());
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    if (dictionary != null) {
      org.apache.arrow.flatbuf.Field.addDictionary(builder, dictionaryOffset);
    }
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
    org.apache.arrow.flatbuf.Field.addLayout(builder, layoutOffset);
    return org.apache.arrow.flatbuf.Field.endField(builder);
  }

  public String getName() {
    return name;
  }

  public boolean isNullable() {
    return nullable;
  }

  public ArrowType getType() {
    return type;
  }

  @JsonInclude(Include.NON_NULL)
  public Long getDictionary() { return dictionary; }

  public List<Field> getChildren() {
    return children;
  }

  public TypeLayout getTypeLayout() {
    return typeLayout;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Field)) {
      return false;
    }
    Field that = (Field) obj;
    return Objects.equals(this.name, that.name) &&
            Objects.equals(this.nullable, that.nullable) &&
            Objects.equals(this.type, that.type) &&
           Objects.equals(this.dictionary, that.dictionary) &&
            (Objects.equals(this.children, that.children) ||
                    (this.children == null && that.children.size() == 0) ||
                    (this.children.size() == 0 && that.children == null));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (name != null) {
      sb.append(name).append(": ");
    }
    sb.append(type);
    if (dictionary != null) {
      sb.append("[dictionary: ").append(dictionary).append("]");
    }
    if (!children.isEmpty()) {
      sb.append("<").append(Joiner.on(", ").join(children)).append(">");
    }
    return sb.toString();
  }
}

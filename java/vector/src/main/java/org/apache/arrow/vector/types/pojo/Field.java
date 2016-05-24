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


import com.google.common.collect.ImmutableList;
import com.google.flatbuffers.FlatBufferBuilder;

import java.util.List;
import java.util.Objects;

import static org.apache.arrow.vector.types.pojo.ArrowType.getTypeForField;

public class Field {
  private final String name;
  private final boolean nullable;
  private final ArrowType type;
  private final List<Field> children;

  public Field(String name, boolean nullable, ArrowType type, List<Field> children) {
    this.name = name;
    this.nullable = nullable;
    this.type = type;
    if (children == null) {
      this.children = ImmutableList.of();
    } else {
      this.children = children;
    }
  }

  public static Field convertField(org.apache.arrow.flatbuf.Field field) {
    String name = field.name();
    boolean nullable = field.nullable();
    ArrowType type = getTypeForField(field);
    ImmutableList.Builder<Field> childrenBuilder = ImmutableList.builder();
    for (int i = 0; i < field.childrenLength(); i++) {
      childrenBuilder.add(convertField(field.children(i)));
    }
    List<Field> children = childrenBuilder.build();
    return new Field(name, nullable, type, children);
  }

  public int getField(FlatBufferBuilder builder) {
    int nameOffset = builder.createString(name);
    int typeOffset = type.getType(builder);
    int[] childrenData = new int[children.size()];
    for (int i = 0; i < children.size(); i++) {
      childrenData[i] = children.get(i).getField(builder);
    }
    int childrenOffset = org.apache.arrow.flatbuf.Field.createChildrenVector(builder, childrenData);
    org.apache.arrow.flatbuf.Field.startField(builder);
    org.apache.arrow.flatbuf.Field.addName(builder, nameOffset);
    org.apache.arrow.flatbuf.Field.addNullable(builder, nullable);
    org.apache.arrow.flatbuf.Field.addTypeType(builder, type.getTypeType());
    org.apache.arrow.flatbuf.Field.addType(builder, typeOffset);
    org.apache.arrow.flatbuf.Field.addChildren(builder, childrenOffset);
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

  public List<Field> getChildren() {
    return children;
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
            (Objects.equals(this.children, that.children) ||
                    (this.children == null && that.children.size() == 0) ||
                    (this.children.size() == 0 && that.children == null));

  }
}

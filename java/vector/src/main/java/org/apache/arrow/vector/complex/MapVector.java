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

package org.apache.arrow.vector.complex;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

import java.util.List;

public class MapVector extends ListVector {

  public static final String KEY_NAME = "key";
  public static final String VALUE_NAME = "value";

  public static MapVector empty(String name, BufferAllocator allocator, boolean keysSorted) {
    return new MapVector(name, allocator, FieldType.nullable(new Map(keysSorted)), null);
  }

  public MapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, fieldType, callBack);
    reader = new UnionMapReader(this);
  }

  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    if (children.size() != 1) {
      throw new IllegalArgumentException("Maps have one List child. Found: " + children);
    }

    Field structField = children.get(0);
    MinorType minorType = Types.getMinorTypeForArrowType(structField.getType());
    if (minorType != MinorType.STRUCT || structField.isNullable()) {
      throw new IllegalArgumentException("Map data should be a non-nullable struct type");
    }

    if (structField.getChildren().size() != 2) {
      throw new IllegalArgumentException("Map data should be a struct with 2 children. Found: " + children);
    }

    Field keyField = structField.getChildren().get(0);
    if (keyField.isNullable()) {
      throw new IllegalArgumentException("Map data key type should be a non-nullable");
    }

    AddOrGetResult<FieldVector> addOrGetVector = addOrGetVector(structField.getFieldType());
    if (!addOrGetVector.isCreated()) {
      throw new IllegalArgumentException("Child vector already existed: " + addOrGetVector.getVector());
    }

    addOrGetVector.getVector().initializeChildrenFromFields(structField.getChildren());
  }

  @Override
  public UnionMapWriter getWriter() {
    return new UnionMapWriter(this);
  }

  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {
    AddOrGetResult<T> result = super.addOrGetVector(fieldType);
    reader = new UnionMapReader(this);
    return result;
  }
}

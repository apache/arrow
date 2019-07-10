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

import static org.apache.arrow.util.Preconditions.checkArgument;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.AddOrGetResult;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;

/**
 * A MapVector is used to store entries of key/value pairs. It is a container vector that is
 * composed of a list of struct values with "key" and "value" fields. The MapVector is nullable,
 * but if a map is set at a given index, there must be an entry. In other words, the StructVector
 * data is non-nullable. Also for a given entry, the "key" is non-nullable, however the "value" can
 * be null.
 */
public class MapVector extends ListVector {

  public static final String KEY_NAME = "key";
  public static final String VALUE_NAME = "value";
  public static final String DATA_VECTOR_NAME = "entries";

  /**
   * Construct an empty MapVector with no data. Child vectors must be added subsequently.
   *
   * @param name The name of the vector.
   * @param allocator The allocator used for allocating/reallocating buffers.
   * @param keysSorted True if the map keys have been pre-sorted.
   * @return a new instance of MapVector.
   */
  public static MapVector empty(String name, BufferAllocator allocator, boolean keysSorted) {
    return new MapVector(name, allocator, FieldType.nullable(new Map(keysSorted)), null);
  }

  /**
   * Construct a MapVector instance.
   *
   * @param name The name of the vector.
   * @param allocator The allocator used for allocating/reallocating buffers.
   * @param fieldType The type definition of the MapVector.
   * @param callBack A schema change callback.
   */
  public MapVector(String name, BufferAllocator allocator, FieldType fieldType, CallBack callBack) {
    super(name, allocator, fieldType, callBack);
    defaultDataVectorName = DATA_VECTOR_NAME;
  }

  /**
   * Initialize child vectors of the map from the given list of fields.
   *
   * @param children List of fields that will be children of this MapVector.
   */
  @Override
  public void initializeChildrenFromFields(List<Field> children) {
    checkArgument(children.size() == 1, "Maps have one List child. Found: " + children);

    Field structField = children.get(0);
    MinorType minorType = Types.getMinorTypeForArrowType(structField.getType());
    checkArgument(minorType == MinorType.STRUCT && !structField.isNullable(),
        "Map data should be a non-nullable struct type");
    checkArgument(structField.getChildren().size() == 2,
        "Map data should be a struct with 2 children. Found: " + children);

    Field keyField = structField.getChildren().get(0);
    checkArgument(!keyField.isNullable(), "Map data key type should be a non-nullable");

    AddOrGetResult<FieldVector> addOrGetVector = addOrGetVector(structField.getFieldType());
    checkArgument(addOrGetVector.isCreated(), "Child vector already existed: " + addOrGetVector.getVector());

    addOrGetVector.getVector().initializeChildrenFromFields(structField.getChildren());
  }

  /**
   * Get the writer for this MapVector instance.
   */
  @Override
  public UnionMapWriter getWriter() {
    return new UnionMapWriter(this);
  }

  /**
   * Get the reader for this MapVector instance.
   */
  @Override
  public UnionMapReader getReader() {
    return (UnionMapReader)reader;
  }

  @Override
  protected void createReader() {
    reader = new UnionMapReader(this);
  }
}

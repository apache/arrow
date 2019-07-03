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
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.ZeroVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.complex.impl.UnionMapWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.ArrowType.Map;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.SchemaChangeRuntimeException;

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

  // TODO: this is only used for addOrGetVector because ListVector declares it private
  protected CallBack callBack;

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
    this.callBack = callBack;
    reader = new UnionMapReader(this);
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

  /**
   * Add a child vector that will be the list vector, or get the vector if already added.
   *
   * @param fieldType The field type of the child vector.
   * @param <T> Type of the resulting vector.
   * @return return an AddOrGetResult instance that contains the vector and created flag.
   */
  @Override
  public <T extends ValueVector> AddOrGetResult<T> addOrGetVector(FieldType fieldType) {

    // TODO: can call super method once DATA_VECTOR_NAME is configurable
    boolean created = false;
    if (vector instanceof ZeroVector) {
      vector = fieldType.createNewSingleVector("entries", allocator, callBack);
      // returned vector must have the same field
      created = true;
      if (callBack != null &&
              // not a schema change if changing from ZeroVector to ZeroVector
              (fieldType.getType().getTypeID() != ArrowTypeID.Null)) {
        callBack.doWork();
      }
    }

    if (vector.getField().getType().getTypeID() != fieldType.getType().getTypeID()) {
      final String msg = String.format("Inner vector type mismatch. Requested type: [%s], actual type: [%s]",
              fieldType.getType().getTypeID(), vector.getField().getType().getTypeID());
      throw new SchemaChangeRuntimeException(msg);
    }

    reader = new UnionMapReader(this);

    return new AddOrGetResult<>((T) vector, created);
  }

  /**
   * Promote this MapVector to a UnionVector.
   *
   * @return the new UnionVector.
   */
  @Override
  public UnionVector promoteToUnion() {
    UnionVector result = super.promoteToUnion();
    reader = new UnionMapReader(this);
    return result;
  }
}

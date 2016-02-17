/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.complex;

import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.MaterializedField;
import org.apache.arrow.vector.types.Types.DataMode;
import org.apache.arrow.vector.types.Types.MajorType;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.CallBack;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Base class for composite vectors.
 *
 * This class implements common functionality of composite vectors.
 */
public abstract class AbstractContainerVector implements ValueVector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AbstractContainerVector.class);

  protected MaterializedField field;
  protected final BufferAllocator allocator;
  protected final CallBack callBack;

  protected AbstractContainerVector(MaterializedField field, BufferAllocator allocator, CallBack callBack) {
    this.field = Preconditions.checkNotNull(field);
    this.allocator = allocator;
    this.callBack = callBack;
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    if (!allocateNewSafe()) {
      throw new OutOfMemoryException();
    }
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  /**
   * Returns the field definition of this instance.
   */
  @Override
  public MaterializedField getField() {
    return field;
  }

  /**
   * Returns a {@link org.apache.arrow.vector.ValueVector} corresponding to the given field name if exists or null.
   */
  public ValueVector getChild(String name) {
    return getChild(name, ValueVector.class);
  }

  /**
   * Returns a sequence of field names in the order that they show up in the schema.
   */
  protected Collection<String> getChildFieldNames() {
    return Sets.newLinkedHashSet(Iterables.transform(field.getChildren(), new Function<MaterializedField, String>() {
      @Nullable
      @Override
      public String apply(MaterializedField field) {
        return Preconditions.checkNotNull(field).getLastName();
      }
    }));
  }

  /**
   * Clears out all underlying child vectors.
   */
 @Override
  public void close() {
    for (ValueVector vector:(Iterable<ValueVector>)this) {
      vector.close();
    }
  }

  protected <T extends ValueVector> T typeify(ValueVector v, Class<T> clazz) {
    if (clazz.isAssignableFrom(v.getClass())) {
      return (T) v;
    }
    throw new IllegalStateException(String.format("Vector requested [%s] was different than type stored [%s].  Drill doesn't yet support hetergenous types.", clazz.getSimpleName(), v.getClass().getSimpleName()));
  }

  MajorType getLastPathType() {
    if((this.getField().getType().getMinorType() == MinorType.LIST  &&
        this.getField().getType().getMode() == DataMode.REPEATED)) {  // Use Repeated scalar type instead of Required List.
      VectorWithOrdinal vord = getChildVectorWithOrdinal(null);
      ValueVector v = vord.vector;
      if (! (v instanceof  AbstractContainerVector)) {
        return v.getField().getType();
      }
    } else if (this.getField().getType().getMinorType() == MinorType.MAP  &&
        this.getField().getType().getMode() == DataMode.REPEATED) {  // Use Required Map
      return new MajorType(MinorType.MAP, DataMode.REQUIRED);
    }

    return this.getField().getType();
  }

  protected boolean supportsDirectRead() {
    return false;
  }

  // return the number of child vectors
  public abstract int size();

  // add a new vector with the input MajorType or return the existing vector if we already added one with the same type
  public abstract <T extends ValueVector> T addOrGet(String name, MajorType type, Class<T> clazz);

  // return the child vector with the input name
  public abstract <T extends ValueVector> T getChild(String name, Class<T> clazz);

  // return the child vector's ordinal in the composite container
  public abstract VectorWithOrdinal getChildVectorWithOrdinal(String name);
}
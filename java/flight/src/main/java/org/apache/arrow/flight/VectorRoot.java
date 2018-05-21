/*
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
package org.apache.arrow.flight;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.common.collect.ImmutableList;

public class VectorRoot extends VectorSchemaRoot {

  public VectorRoot(Iterable<FieldVector> vectors) {
    super(
        ImmutableList.copyOf(vectors).stream().map(t -> t.getField()).collect(Collectors.toList()),
        ImmutableList.copyOf(vectors),
        0
        );
  }

  public VectorRoot(FieldVector...vectors) {
    this(ImmutableList.copyOf(vectors));
  }

  public void allocateNew() {
    for(FieldVector v : getFieldVectors()) {
      v.allocateNew();
    }
  }

  public void setValueCount(int count) {
    for(FieldVector v : getFieldVectors()) {
      v.setValueCount(count);
    }
  }

  public void clear() {
    for(FieldVector v : getFieldVectors()) {
      v.clear();
    }
  }

  public static VectorRoot create(Schema schema, BufferAllocator allocator) {
    List<FieldVector> fieldVectors = new ArrayList<>();
    for (Field field : schema.getFields()) {
      FieldVector vector = field.createVector(allocator);
      fieldVectors.add(vector);
    }
    if (fieldVectors.size() != schema.getFields().size()) {
      throw new IllegalArgumentException("The root vector did not create the right number of children. found " +
          fieldVectors.size() + " expected " + schema.getFields().size());
    }
    return new VectorRoot(fieldVectors);
  }
}

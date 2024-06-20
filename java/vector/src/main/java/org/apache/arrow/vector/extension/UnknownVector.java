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
package org.apache.arrow.vector.extension;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueIterableVector;
import org.apache.arrow.vector.types.pojo.Field;

public class UnknownVector extends ExtensionTypeVector<FieldVector>
    implements ValueIterableVector<Object> {
  public UnknownVector(String name, BufferAllocator allocator, FieldVector underlyingVector) {
    super(name, allocator, underlyingVector);
  }

  public UnknownVector(Field field, BufferAllocator allocator, FieldVector underlyingVector) {
    super(field, allocator, underlyingVector);
  }

  // TODO: getField

  @Override
  public Object getObject(int index) {
    return getUnderlyingVector().getObject(index);
  }

  @Override
  public int hashCode(int index) {
    // TODO:
    return 0;
  }

  @Override
  public int hashCode(int index, ArrowBufHasher hasher) {
    // TODO:
    return 0;
  }
}

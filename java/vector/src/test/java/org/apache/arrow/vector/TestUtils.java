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

package org.apache.arrow.vector;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public class TestUtils {

  public static VarCharVector newVarCharVector(String name, BufferAllocator allocator) {
    return (VarCharVector)
        FieldType.nullable(new ArrowType.Utf8()).createNewSingleVector(name, allocator, null);
  }

  public static ViewVarCharVector newViewVarCharVector(String name, BufferAllocator allocator) {
    return (ViewVarCharVector)
        FieldType.nullable(new ArrowType.Utf8View()).createNewSingleVector(name, allocator, null);
  }

  public static VarBinaryVector newVarBinaryVector(String name, BufferAllocator allocator) {
    return (VarBinaryVector)
        FieldType.nullable(new ArrowType.Binary()).createNewSingleVector(name, allocator, null);
  }

  public static ViewVarBinaryVector newViewVarBinaryVector(String name, BufferAllocator allocator) {
    return (ViewVarBinaryVector)
            FieldType.nullable(new ArrowType.BinaryView()).createNewSingleVector(name, allocator, null);
  }

  public static <T> T newVector(Class<T> c, String name, ArrowType type, BufferAllocator allocator) {
    return c.cast(FieldType.nullable(type).createNewSingleVector(name, allocator, null));
  }

  public static <T> T newVector(Class<T> c, String name, MinorType type, BufferAllocator allocator) {
    return c.cast(FieldType.nullable(type.getType()).createNewSingleVector(name, allocator, null));
  }

}

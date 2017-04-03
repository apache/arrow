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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.util.CallBack;

public class FieldType {

  public static FieldType nullable(ArrowType type) {
    return new FieldType(true, type, null);
  }

  private final boolean nullable;
  private final ArrowType type;
  private final DictionaryEncoding dictionary;

  public FieldType(boolean nullable, ArrowType type, DictionaryEncoding dictionary) {
    super();
    this.nullable = nullable;
    this.type = checkNotNull(type);
    this.dictionary = dictionary;
  }

  public boolean isNullable() {
    return nullable;
  }
  public ArrowType getType() {
    return type;
  }
  public DictionaryEncoding getDictionary() {
    return dictionary;
  }

  public FieldVector createNewSingleVector(String name, BufferAllocator allocator, CallBack schemaCallBack) {
    MinorType minorType = Types.getMinorTypeForArrowType(type);
    return minorType.getNewVector(name, this, allocator, schemaCallBack);
  }

}

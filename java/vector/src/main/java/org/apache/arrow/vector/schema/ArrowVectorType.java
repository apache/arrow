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

package org.apache.arrow.vector.schema;

import java.util.Map;

import org.apache.arrow.flatbuf.VectorType;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class ArrowVectorType {

  public static final ArrowVectorType DATA = new ArrowVectorType(VectorType.DATA);
  public static final ArrowVectorType OFFSET = new ArrowVectorType(VectorType.OFFSET);
  public static final ArrowVectorType VALIDITY = new ArrowVectorType(VectorType.VALIDITY);
  public static final ArrowVectorType TYPE = new ArrowVectorType(VectorType.TYPE);

  private static final Map<String, ArrowVectorType> typeByName;

  static {
    ArrowVectorType[] types = {DATA, OFFSET, VALIDITY, TYPE};
    Builder<String, ArrowVectorType> builder = ImmutableMap.builder();
    for (ArrowVectorType type : types) {
      builder.put(type.getName(), type);
    }
    typeByName = builder.build();
  }

  public static ArrowVectorType fromName(String name) {
    ArrowVectorType type = typeByName.get(name);
    if (type == null) {
      throw new IllegalArgumentException("Unknown type " + name);
    }
    return type;
  }

  private final short type;

  public ArrowVectorType(short type) {
    this.type = type;
    // validate that the type is valid
    getName();
  }

  @JsonCreator
  private ArrowVectorType(String name) {
    this.type = fromName(name).type;
  }

  public short getType() {
    return type;
  }

  @JsonValue
  public String getName() {
    try {
      return VectorType.name(type);
    } catch (ArrayIndexOutOfBoundsException e) {
      throw new IllegalArgumentException("Unknown type " + type);
    }
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public int hashCode() {
    return type;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ArrowVectorType) {
      ArrowVectorType other = (ArrowVectorType) obj;
      return type == other.type;
    }
    return false;
  }

}

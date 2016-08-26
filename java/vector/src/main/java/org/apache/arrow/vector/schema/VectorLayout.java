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

import static org.apache.arrow.vector.schema.ArrowVectorType.OFFSET;
import static org.apache.arrow.vector.schema.ArrowVectorType.TYPE;
import static org.apache.arrow.vector.schema.ArrowVectorType.VALIDITY;
import static org.apache.arrow.vector.schema.ArrowVectorType.VALUES;

public class VectorLayout {

  private static final VectorLayout VALIDITY_VECTOR = new VectorLayout(VALIDITY, 1);
  private static final VectorLayout OFFSET_VECTOR = new VectorLayout(OFFSET, 32);
  private static final VectorLayout TYPE_VECTOR = new VectorLayout(TYPE, 32);
  private static final VectorLayout BOOLEAN_VECTOR = new VectorLayout(VALUES, 1);
  private static final VectorLayout VALUES_64 = new VectorLayout(VALUES, 64);
  private static final VectorLayout VALUES_32 = new VectorLayout(VALUES, 32);
  private static final VectorLayout VALUES_16 = new VectorLayout(VALUES, 16);
  private static final VectorLayout VALUES_8 = new VectorLayout(VALUES, 8);

  public static VectorLayout typeVector() {
    return TYPE_VECTOR;
  }

  public static VectorLayout offsetVector() {
    return OFFSET_VECTOR;
  }

  public static VectorLayout dataVector(int typeBitWidth) {
    switch (typeBitWidth) {
    case 8:
      return VALUES_8;
    case 16:
      return VALUES_16;
    case 32:
      return VALUES_32;
    case 64:
      return VALUES_64;
    default:
      throw new IllegalArgumentException("only 8, 16, 32, or 64 bits supported");
    }
  }

  public static VectorLayout booleanVector() {
    return BOOLEAN_VECTOR;
  }

  public static VectorLayout validityVector() {
    return VALIDITY_VECTOR;
  }

  public static VectorLayout byteVector() {
    return dataVector(8);
  }

  private final int typeBitWidth;

  private final ArrowVectorType type;

  private VectorLayout(ArrowVectorType type, int typeBitWidth) {
    super();
    this.type = type;
    this.typeBitWidth = typeBitWidth;
  }

  public int getTypeBitWidth() {
    return typeBitWidth;
  }

  public ArrowVectorType getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("{width=%s,type=%s}", typeBitWidth, type);
  }
}

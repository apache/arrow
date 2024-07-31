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
package org.apache.arrow.vector.types;

import org.apache.arrow.flatbuf.Precision;

/** Precisions of primitive floating point numbers. */
public enum FloatingPointPrecision {
  /** 16-bit (not a standard java type). */
  HALF(Precision.HALF),
  /** 32-bit (i.e. float in java). */
  SINGLE(Precision.SINGLE),
  /** 64-bit (i.e. double in java). */
  DOUBLE(Precision.DOUBLE);

  private static final FloatingPointPrecision[] valuesByFlatbufId =
      new FloatingPointPrecision[FloatingPointPrecision.values().length];

  static {
    for (FloatingPointPrecision v : FloatingPointPrecision.values()) {
      valuesByFlatbufId[v.flatbufID] = v;
    }
  }

  private final short flatbufID;

  private FloatingPointPrecision(short flatbufID) {
    this.flatbufID = flatbufID;
  }

  public short getFlatbufID() {
    return flatbufID;
  }

  public static FloatingPointPrecision fromFlatbufID(short id) {
    return valuesByFlatbufId[id];
  }
}

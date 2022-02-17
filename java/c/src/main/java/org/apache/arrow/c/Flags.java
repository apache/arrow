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

package org.apache.arrow.c;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Flags as defined in the C data interface specification.
 */
final class Flags {
  static final int ARROW_FLAG_DICTIONARY_ORDERED = 1;
  static final int ARROW_FLAG_NULLABLE = 2;
  static final int ARROW_FLAG_MAP_KEYS_SORTED = 4;

  private Flags() {
  }

  static long forField(Field field) {
    long flags = 0L;
    if (field.isNullable()) {
      flags |= ARROW_FLAG_NULLABLE;
    }
    if (field.getDictionary() != null && field.getDictionary().isOrdered()) {
      flags |= ARROW_FLAG_DICTIONARY_ORDERED;
    }
    if (field.getType().getTypeID() == ArrowTypeID.Map) {
      ArrowType.Map map = (ArrowType.Map) field.getType();
      if (map.getKeysSorted()) {
        flags |= ARROW_FLAG_MAP_KEYS_SORTED;
      }
    }
    return flags;
  }
}

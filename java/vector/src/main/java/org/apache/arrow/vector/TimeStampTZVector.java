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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * TimeStampTZVector is an abstract interface for timestamp vectors with a timezone.
 */
public abstract class TimeStampTZVector extends TimeStampVector {

  protected final String timeZone;

  /**
   * Instantiate a TimeStampTZVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param name name of the vector
   * @param fieldType type of Field materialized by this vector
   * @param allocator allocator for memory management.
   */
  protected TimeStampTZVector(String name, FieldType fieldType, BufferAllocator allocator) {
    super(name, fieldType, allocator);
    ArrowType.Timestamp arrowType = (ArrowType.Timestamp) fieldType.getType();
    this.timeZone = arrowType.getTimezone();
  }

  /**
   * Instantiate a TimeStampTZVector. This doesn't allocate any memory for
   * the data in vector.
   *
   * @param field field materialized by this vector
   * @param allocator allocator for memory management.
   */
  protected TimeStampTZVector(Field field, BufferAllocator allocator) {
    super(field, allocator);
    ArrowType.Timestamp arrowType = (ArrowType.Timestamp) field.getFieldType().getType();
    this.timeZone = arrowType.getTimezone();
  }

  /**
   * Get the time zone of the timestamps stored in this vector.
   *
   * @return the time zone of the timestamps stored in this vector.
   */
  public String getTimeZone() {
    return timeZone;
  }
}

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

package org.apache.arrow.consumers;

import java.io.IOException;
import java.util.Map;

import org.apache.arrow.AvroToArrowUtils;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.avro.io.Decoder;

/**
 * An abstraction that is used to consume values from avro decoder.
 */
public abstract class Consumer {

  /**
   * Indicates whether has null value.
   */
  protected boolean nullable;

  /**
   * Null field index in avro schema.
   */
  protected int nullIndex;

  public abstract void consume(Decoder decoder) throws IOException;

  /**
   * Get avro null field index from vector field metadata.
   */
  protected void getNullFieldIndex(Field field) {
    Map<String, String> metadata = field.getMetadata();
    Preconditions.checkNotNull(metadata, "metadata should not be null when vector is nullable");
    String index = metadata.get(AvroToArrowUtils.NULL_INDEX);
    Preconditions.checkNotNull(index, "nullIndex should not be null when vector is nullable");
    nullIndex = Integer.parseInt(index);
  }
}

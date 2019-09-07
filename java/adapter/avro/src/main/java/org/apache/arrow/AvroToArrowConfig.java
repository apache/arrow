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

package org.apache.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * This class configures the Avro-to-Arrow conversion process.
 */
public class AvroToArrowConfig {

  private final BufferAllocator allocator;
  /**
   * The maximum rowCount to read each time when partially convert data.
   * Default value is 1024 and -1 means read all data into one vector.
   */
  private final int targetBatchSize;

  /**
   * The dictionary provider used for enum type.
   * If avro schema has enum type, will create dictionary and update this provider.
   */
  private final DictionaryProvider.MapDictionaryProvider provider;

  public AvroToArrowConfig(BufferAllocator allocator) {
    this (allocator, AvroToArrowVectorIterator.DEFAULT_BATCH_SIZE);
  }

  public AvroToArrowConfig(BufferAllocator allocator, int targetBatchSize) {
    this (allocator, targetBatchSize, new DictionaryProvider.MapDictionaryProvider());
  }

  /**
   * Instantiate an instance.
   * @param allocator The memory allocator to construct the Arrow vectors with.
   * @param targetBatchSize The maximum rowCount to read each time when partially convert data.
   * @param provider The dictionary provider used for enum type, adapter will update this provider.
   */
  public AvroToArrowConfig(
      BufferAllocator allocator,
      int targetBatchSize,
      DictionaryProvider.MapDictionaryProvider provider) {

    Preconditions.checkArgument(targetBatchSize == AvroToArrowVectorIterator.NO_LIMIT_BATCH_SIZE ||
        targetBatchSize > 0, "invalid targetBatchSize: %s", targetBatchSize);

    this.allocator = allocator;
    this.targetBatchSize = targetBatchSize;
    this.provider = provider;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public int getTargetBatchSize() {
    return targetBatchSize;
  }

  public DictionaryProvider.MapDictionaryProvider getProvider() {
    return provider;
  }
}

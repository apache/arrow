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

package org.apache.arrow.adapter.avro;

import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.DictionaryProvider;

/**
 * This class builds {@link AvroToArrowConfig}s.
 */
public class AvroToArrowConfigBuilder {

  private BufferAllocator allocator;

  private int targetBatchSize;

  private DictionaryProvider.MapDictionaryProvider provider;

  private Set<String> skipFieldNames;

  /**
   * Default constructor for the {@link AvroToArrowConfigBuilder}.
   */
  public AvroToArrowConfigBuilder(BufferAllocator allocator) {
    this.allocator = allocator;
    this.targetBatchSize = AvroToArrowVectorIterator.DEFAULT_BATCH_SIZE;
    this.provider = new DictionaryProvider.MapDictionaryProvider();
    this.skipFieldNames = new HashSet<>();
  }

  public AvroToArrowConfigBuilder setTargetBatchSize(int targetBatchSize) {
    this.targetBatchSize = targetBatchSize;
    return this;
  }

  public AvroToArrowConfigBuilder setProvider(DictionaryProvider.MapDictionaryProvider provider) {
    this.provider = provider;
    return this;
  }

  public AvroToArrowConfigBuilder setSkipFieldNames(Set<String> skipFieldNames) {
    this.skipFieldNames = skipFieldNames;
    return this;
  }

  /**
   * This builds the {@link AvroToArrowConfig} from the provided params.
   */
  public AvroToArrowConfig build() {
    return new AvroToArrowConfig(
        allocator,
        targetBatchSize,
        provider,
        skipFieldNames);
  }
}

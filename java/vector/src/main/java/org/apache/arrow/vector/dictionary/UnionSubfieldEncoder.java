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

package org.apache.arrow.vector.dictionary;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.UnionVector;

/**
 * Sub fields encoder/decoder for Dictionary encoded {@link UnionVector}.
 * Notes that child vectors within struct vector can either be dictionary encodeable or not.
 */
public class UnionSubfieldEncoder extends StructSubfieldEncoder {

  /**
   * Construct an instance.
   */
  public UnionSubfieldEncoder(
      BufferAllocator allocator,
      DictionaryProvider.MapDictionaryProvider provider) {

    super(allocator, provider);
  }

  @Override
  protected void checkVectorType(FieldVector vector) {
    Preconditions.checkArgument(vector instanceof UnionVector);
  }
}

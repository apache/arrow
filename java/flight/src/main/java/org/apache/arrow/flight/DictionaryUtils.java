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

package org.apache.arrow.flight;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowDictionaryBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.DictionaryUtility;

/**
 * Utilities to work with dictionaries in Flight.
 */
final class DictionaryUtils {

  private DictionaryUtils() {
    throw new UnsupportedOperationException("Do not instantiate this class.");
  }

  /**
   * Generate all the necessary Flight messages to send a schema and associated dictionaries.
   */
  static Schema generateSchemaMessages(final Schema originalSchema, final FlightDescriptor descriptor,
      final DictionaryProvider provider, final Consumer<ArrowMessage> messageCallback) {
    final List<Field> fields = new ArrayList<>(originalSchema.getFields().size());
    final Set<Long> dictionaryIds = new HashSet<>();
    for (final Field field : originalSchema.getFields()) {
      fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIds));
    }
    final Schema schema = new Schema(fields, originalSchema.getCustomMetadata());
    // Send the schema message
    messageCallback.accept(new ArrowMessage(descriptor == null ? null : descriptor.toProtocol(), schema));
    // Create and write dictionary batches
    for (Long id : dictionaryIds) {
      final Dictionary dictionary = provider.lookup(id);
      final FieldVector vector = dictionary.getVector();
      final int count = vector.getValueCount();
      // Do NOT close this root, as it does not actually own the vector.
      final VectorSchemaRoot dictRoot = new VectorSchemaRoot(
          Collections.singletonList(vector.getField()),
          Collections.singletonList(vector),
          count);
      final VectorUnloader unloader = new VectorUnloader(dictRoot);
      try (final ArrowDictionaryBatch dictionaryBatch = new ArrowDictionaryBatch(
          id, unloader.getRecordBatch())) {
        messageCallback.accept(new ArrowMessage(dictionaryBatch));
      }
    }
    return schema;
  }
}

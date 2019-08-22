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
import java.util.stream.Collectors;

import org.apache.arrow.util.AutoCloseables;
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
    final Set<Long> dictionaryIds = new HashSet<>();
    final Schema schema = generateSchema(originalSchema, provider, dictionaryIds);
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

  static void closeDictionaries(final Schema schema, final DictionaryProvider provider) throws Exception {
    // Close dictionaries
    final Set<Long> dictionaryIds = new HashSet<>();
    schema.getFields().forEach(field -> DictionaryUtility.toMessageFormat(field, provider, dictionaryIds));

    final List<AutoCloseable> dictionaryVectors = dictionaryIds.stream()
            .map(id -> (AutoCloseable) provider.lookup(id).getVector()).collect(Collectors.toList());
    AutoCloseables.close(dictionaryVectors);
  }

  /**
   * Generates the schema to send with flight messages.
   * If the schema contains no field with a dictionary, it will return the schema as is.
   * Otherwise, it will return a newly created a new schema after converting the fields.
   * @param originalSchema the original schema.
   * @param provider the dictionary provider.
   * @param dictionaryIds dictionary IDs that are used.
   * @return the schema to send with the flight messages.
   */
  static Schema generateSchema(
          final Schema originalSchema, final DictionaryProvider provider, Set<Long> dictionaryIds) {
    // first determine if a new schema needs to be created.
    boolean createSchema = false;
    for (Field field : originalSchema.getFields()) {
      if (DictionaryUtility.needConvertToMessageFormat(field)) {
        createSchema = true;
        break;
      }
    }

    if (!createSchema) {
      return originalSchema;
    } else {
      final List<Field> fields = new ArrayList<>(originalSchema.getFields().size());
      for (final Field field : originalSchema.getFields()) {
        fields.add(DictionaryUtility.toMessageFormat(field, provider, dictionaryIds));
      }
      return new Schema(fields, originalSchema.getCustomMetadata());
    }
  }
}

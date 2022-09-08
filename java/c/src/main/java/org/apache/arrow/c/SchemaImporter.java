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

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Importer for {@link ArrowSchema}.
 */
final class SchemaImporter {
  private static final Logger logger = LoggerFactory.getLogger(SchemaImporter.class);

  private static final int MAX_IMPORT_RECURSION_LEVEL = 64;
  private long nextDictionaryID = 1L;

  private final BufferAllocator allocator;

  public SchemaImporter(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  Field importField(ArrowSchema schema, CDataDictionaryProvider provider) {
    return importField(schema, provider, 0);
  }

  private Field importField(ArrowSchema schema, CDataDictionaryProvider provider, int recursionLevel) {
    checkState(recursionLevel <= MAX_IMPORT_RECURSION_LEVEL, "Recursion level in ArrowSchema struct exceeded");

    ArrowSchema.Snapshot snapshot = schema.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowSchema");

    String name = NativeUtil.toJavaString(snapshot.name);
    String format = NativeUtil.toJavaString(snapshot.format);
    checkNotNull(format, "format field must not be null");
    ArrowType arrowType = Format.asType(format, snapshot.flags);
    boolean nullable = (snapshot.flags & Flags.ARROW_FLAG_NULLABLE) != 0;
    Map<String, String> metadata = Metadata.decode(snapshot.metadata);

    if (metadata != null && metadata.containsKey(ExtensionType.EXTENSION_METADATA_KEY_NAME)) {
      final String extensionName = metadata.get(ExtensionType.EXTENSION_METADATA_KEY_NAME);
      final String extensionMetadata = metadata.getOrDefault(ExtensionType.EXTENSION_METADATA_KEY_METADATA, "");
      ExtensionType extensionType = ExtensionTypeRegistry.lookup(extensionName);
      if (extensionType != null) {
        arrowType = extensionType.deserialize(arrowType, extensionMetadata);
      } else {
        // Otherwise, we haven't registered the type
        logger.info("Unrecognized extension type: {}", extensionName);
      }
    }

    // Handle dictionary encoded vectors
    DictionaryEncoding dictionaryEncoding = null;
    if (snapshot.dictionary != NULL && provider != null) {
      boolean ordered = (snapshot.flags & Flags.ARROW_FLAG_DICTIONARY_ORDERED) != 0;
      ArrowType.Int indexType = (ArrowType.Int) arrowType;
      dictionaryEncoding = new DictionaryEncoding(nextDictionaryID++, ordered, indexType);

      ArrowSchema dictionarySchema = ArrowSchema.wrap(snapshot.dictionary);
      Field dictionaryField = importField(dictionarySchema, provider, recursionLevel + 1);
      provider.put(new Dictionary(dictionaryField.createVector(allocator), dictionaryEncoding));
    }

    FieldType fieldType = new FieldType(nullable, arrowType, dictionaryEncoding, metadata);

    List<Field> children = null;
    long[] childrenIds = NativeUtil.toJavaArray(snapshot.children, checkedCastToInt(snapshot.n_children));
    if (childrenIds != null && childrenIds.length > 0) {
      children = new ArrayList<>(childrenIds.length);
      for (long childAddress : childrenIds) {
        ArrowSchema childSchema = ArrowSchema.wrap(childAddress);
        Field field = importField(childSchema, provider, recursionLevel + 1);
        children.add(field);
      }
    }
    return new Field(name, fieldType, children);
  }
}

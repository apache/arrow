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

package org.apache.arrow.vector.types.pojo;

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType.ExtensionType;
import org.apache.arrow.vector.util.CallBack;

/**
 * POJO representation of an Arrow field type.  It consists of a logical type, nullability and whether the field
 * (column) is dictionary encoded.
 */
public class FieldType {

  public static FieldType nullable(ArrowType type) {
    return new FieldType(true, type, null, null);
  }

  private final boolean nullable;
  private final ArrowType type;
  private final DictionaryEncoding dictionary;
  private final Map<String, String> metadata;

  public FieldType(boolean nullable, ArrowType type, DictionaryEncoding dictionary) {
    this(nullable, type, dictionary, null);
  }

  /**
   * Constructs a new instance.
   *
   * @param nullable Whether the Vector is nullable
   * @param type The logical arrow type of the field.
   * @param dictionary The dictionary encoding of the field.
   * @param metadata Custom metadata for the field.
   */
  public FieldType(boolean nullable, ArrowType type, DictionaryEncoding dictionary, Map<String, String> metadata) {
    super();
    this.nullable = nullable;
    this.type = Preconditions.checkNotNull(type);
    this.dictionary = dictionary;
    if (type instanceof ExtensionType) {
      // Save the extension type name/metadata
      final Map<String, String> extensionMetadata = new HashMap<>();
      extensionMetadata.put(ExtensionType.EXTENSION_METADATA_KEY_NAME, ((ExtensionType) type).extensionName());
      extensionMetadata.put(ExtensionType.EXTENSION_METADATA_KEY_METADATA, ((ExtensionType) type).serialize());
      if (metadata != null) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
          extensionMetadata.put(entry.getKey(), entry.getValue());
        }
      }
      this.metadata = Collections2.immutableMapCopy(extensionMetadata);
    } else {
      this.metadata = metadata == null ? java.util.Collections.emptyMap() : Collections2.immutableMapCopy(metadata);
    }
  }

  public boolean isNullable() {
    return nullable;
  }

  public ArrowType getType() {
    return type;
  }

  public DictionaryEncoding getDictionary() {
    return dictionary;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public FieldVector createNewSingleVector(String name, BufferAllocator allocator, CallBack schemaCallBack) {
    MinorType minorType = Types.getMinorTypeForArrowType(type);
    return minorType.getNewVector(name, this, allocator, schemaCallBack);
  }

  public FieldVector createNewSingleVector(Field field, BufferAllocator allocator, CallBack schemaCallBack) {
    MinorType minorType = Types.getMinorTypeForArrowType(type);
    return minorType.getNewVector(field, allocator, schemaCallBack);
  }

}

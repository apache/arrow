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

package org.apache.arrow.vector.validate;

import java.io.IOException;
import java.util.Iterator;

import org.apache.arrow.vector.types.MetadataVersion;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Given a field, checks that no Union fields are present.
 *
 * This is intended to be used to prevent unions from being read/written with V4 metadata.
 */
public final class MetadataV4UnionChecker {
  static boolean isUnion(Field field) {
    return field.getType().getTypeID() == ArrowType.ArrowTypeID.Union;
  }

  static Field check(Field field) {
    if (isUnion(field)) {
      return field;
    }
    // Naive recursive DFS
    for (final Field child : field.getChildren()) {
      final Field result = check(child);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  /**
   * Check the schema, raising an error if an unsupported feature is used (e.g. unions with < V5 metadata).
   */
  public static void checkForUnion(Iterator<Field> fields, MetadataVersion metadataVersion) {
    if (metadataVersion.toFlatbufID() >= MetadataVersion.V5.toFlatbufID()) {
      return;
    }
    while (fields.hasNext()) {
      Field union = check(fields.next());
      if (union != null) {
        throw new IllegalArgumentException(
            "Cannot write union with V4 metadata version, use V5 instead. Found field: " + union);
      }
    }
  }

  /**
   * Check the schema, raising an error if an unsupported feature is used (e.g. unions with < V5 metadata).
   */
  public static void checkRead(Schema schema, MetadataVersion metadataVersion) throws IOException {
    if (metadataVersion.toFlatbufID() >= MetadataVersion.V5.toFlatbufID()) {
      return;
    }
    for (final Field field : schema.getFields()) {
      Field union = check(field);
      if (union != null) {
        throw new IOException("Cannot read union with V4 metadata version. Found field: " + union);
      }
    }
  }
}

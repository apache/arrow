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

package org.apache.arrow.vector.util;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compare.TypeEqualsVisitor;

/**
 * Utility to append {@link org.apache.arrow.vector.VectorSchemaRoot}s with the same schema.
 */
public class VectorSchemaRootAppender {

  /**
   * Appends a number of {@link VectorSchemaRoot}s.
   * @param checkSchema if we need to check schema for the vector schema roots.
   * @param targetRoot the vector schema root to be appended.
   * @param rootsToAppend the vector schema roots to append.
   * @throws IllegalArgumentException throws if we need to check schema, and checking schema fails.
   */
  public static void append(boolean checkSchema, VectorSchemaRoot targetRoot, VectorSchemaRoot... rootsToAppend) {
    // create appenders
    VectorAppender[] appenders = new VectorAppender[targetRoot.getFieldVectors().size()];
    for (int i = 0; i < appenders.length; i++) {
      appenders[i] = new VectorAppender(targetRoot.getVector(i));
    }

    // create type checkers, if necessary
    TypeEqualsVisitor[] typeCheckers = null;
    if (checkSchema) {
      typeCheckers = new TypeEqualsVisitor[targetRoot.getFieldVectors().size()];
      for (int i = 0; i < typeCheckers.length; i++) {
        typeCheckers[i] = new TypeEqualsVisitor(targetRoot.getVector(i),
            /* check name */ false, /* check meta data */ false);
      }
    }

    for (VectorSchemaRoot delta : rootsToAppend) {
      // check schema, if necessary
      if (checkSchema) {
        if (delta.getFieldVectors().size() != targetRoot.getFieldVectors().size()) {
          throw new IllegalArgumentException("Vector schema roots have different numbers of child vectors.");
        }
        for (int i = 0; i < typeCheckers.length; i++) {
          if (!typeCheckers[i].equals(delta.getVector(i))) {
            throw new IllegalArgumentException("Vector schema roots have different schemas.");
          }
        }
      }

      // append child vectors.
      for (int i = 0; i < appenders.length; i++) {
        delta.getVector(i).accept(appenders[i], null);
      }
      targetRoot.setRowCount(targetRoot.getRowCount() + delta.getRowCount());
    }
  }

  /**
   * Appends a number of {@link VectorSchemaRoot}s.
   * This method performs schema checking before appending data.
   * @param targetRoot the vector schema root to be appended.
   * @param rootsToAppend the vector schema roots to append.
   * @throws IllegalArgumentException throws if we need to check schema, and checking schema fails.
   */
  public static void append(VectorSchemaRoot targetRoot, VectorSchemaRoot... rootsToAppend) {
    append(true, targetRoot, rootsToAppend);
  }
}

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

package org.apache.arrow.driver.jdbc.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Converts Arrow's {@link VectorSchemaRoot} format to one JDBC would expect.
 */
@FunctionalInterface
public interface VectorSchemaRootTransformer {
  VectorSchemaRoot transform(VectorSchemaRoot originalRoot, VectorSchemaRoot transformedRoot)
      throws Exception;

  /**
   * Transformer's helper class; builds a new {@link VectorSchemaRoot}.
   */
  class Builder {

    private final Schema schema;
    private final BufferAllocator bufferAllocator;
    private final List<Field> newFields = new ArrayList<>();
    private final Collection<Task> tasks = new ArrayList<>();

    public Builder(final Schema schema, final BufferAllocator bufferAllocator) {
      this.schema = schema;
      this.bufferAllocator = bufferAllocator
          .newChildAllocator("VectorSchemaRootTransformer", 0, bufferAllocator.getLimit());
    }

    /**
     * Add task to transform a vector to a new vector renaming it.
     * This also adds transformedVectorName to the transformed {@link VectorSchemaRoot} schema.
     *
     * @param originalVectorName    Name of the original vector to be transformed.
     * @param transformedVectorName Name of the vector that is the result of the transformation.
     * @return a VectorSchemaRoot instance with a task to rename a field vector.
     */
    public Builder renameFieldVector(final String originalVectorName,
                                     final String transformedVectorName) {
      tasks.add((originalRoot, transformedRoot) -> {
        final FieldVector originalVector = originalRoot.getVector(originalVectorName);
        final FieldVector transformedVector = transformedRoot.getVector(transformedVectorName);

        final ArrowType originalType = originalVector.getField().getType();
        final ArrowType transformedType = transformedVector.getField().getType();
        if (!originalType.equals(transformedType)) {
          throw new IllegalArgumentException(String.format(
              "Can not transfer vector with field type %s to %s", originalType, transformedType));
        }

        if (originalVector instanceof BaseVariableWidthVector) {
          ((BaseVariableWidthVector) originalVector).transferTo(
              ((BaseVariableWidthVector) transformedVector));
        } else if (originalVector instanceof BaseFixedWidthVector) {
          ((BaseFixedWidthVector) originalVector).transferTo(
              ((BaseFixedWidthVector) transformedVector));
        } else {
          throw new IllegalStateException(String.format(
              "Can not transfer vector of type %s", originalVector.getClass()));
        }
      });

      final Field originalField = schema.findField(originalVectorName);
      newFields.add(new Field(
          transformedVectorName,
          new FieldType(originalField.isNullable(), originalField.getType(),
              originalField.getDictionary(), originalField.getMetadata()),
          originalField.getChildren())
      );

      return this;
    }

    /**
     * Adds an empty field to the transformed {@link VectorSchemaRoot} schema.
     *
     * @param fieldName Name of the field to be added.
     * @param fieldType Type of the field to be added.
     * @return a VectorSchemaRoot instance with the current tasks.
     */
    public Builder addEmptyField(final String fieldName, final Types.MinorType fieldType) {
      newFields.add(Field.nullable(fieldName, fieldType.getType()));

      return this;
    }

    /**
     * Adds an empty field to the transformed {@link VectorSchemaRoot} schema.
     *
     * @param fieldName Name of the field to be added.
     * @param fieldType Type of the field to be added.
     * @return a VectorSchemaRoot instance with the current tasks.
     */
    public Builder addEmptyField(final String fieldName, final ArrowType fieldType) {
      newFields.add(Field.nullable(fieldName, fieldType));

      return this;
    }

    public VectorSchemaRootTransformer build() {
      return (originalRoot, transformedRoot) -> {
        if (transformedRoot == null) {
          transformedRoot = VectorSchemaRoot.create(new Schema(newFields), bufferAllocator);
        }

        for (final Task task : tasks) {
          task.run(originalRoot, transformedRoot);
        }

        transformedRoot.setRowCount(originalRoot.getRowCount());

        originalRoot.clear();
        return transformedRoot;
      };
    }

    /**
     * Functional interface used to a task to transform a VectorSchemaRoot into a new VectorSchemaRoot.
     */
    @FunctionalInterface
    interface Task {
      void run(VectorSchemaRoot originalRoot, VectorSchemaRoot transformedRoot);
    }
  }
}

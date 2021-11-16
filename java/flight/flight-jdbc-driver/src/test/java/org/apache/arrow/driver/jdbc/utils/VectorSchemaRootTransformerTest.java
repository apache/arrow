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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class VectorSchemaRootTransformerTest {

  @Rule
  public RootAllocatorTestRule rootAllocatorTestRule = new RootAllocatorTestRule();
  private final BufferAllocator rootAllocator = rootAllocatorTestRule.getRootAllocator();

  @Test
  public void testTransformerBuilderWorksCorrectly() throws Exception {
    final VarBinaryVector field1 = rootAllocatorTestRule.createVarBinaryVector("FIELD_1");
    final VarBinaryVector field2 = rootAllocatorTestRule.createVarBinaryVector("FIELD_2");
    final VarBinaryVector field3 = rootAllocatorTestRule.createVarBinaryVector("FIELD_3");

    try (final VectorSchemaRoot originalRoot = VectorSchemaRoot.of(field1, field2, field3);
         final VectorSchemaRoot clonedRoot = cloneVectorSchemaRoot(originalRoot)) {

      final VectorSchemaRootTransformer.Builder builder =
          new VectorSchemaRootTransformer.Builder(originalRoot.getSchema(),
              rootAllocator);

      builder.renameFieldVector("FIELD_3", "FIELD_3_RENAMED");
      builder.addEmptyField("EMPTY_FIELD", new ArrowType.Bool());
      builder.renameFieldVector("FIELD_2", "FIELD_2_RENAMED");
      builder.renameFieldVector("FIELD_1", "FIELD_1_RENAMED");

      final VectorSchemaRootTransformer transformer = builder.build();

      final Schema transformedSchema = new Schema(ImmutableList.of(
          Field.nullable("FIELD_3_RENAMED", new ArrowType.Binary()),
          Field.nullable("EMPTY_FIELD", new ArrowType.Bool()),
          Field.nullable("FIELD_2_RENAMED", new ArrowType.Binary()),
          Field.nullable("FIELD_1_RENAMED", new ArrowType.Binary())
      ));
      try (final VectorSchemaRoot transformedRoot = createVectorSchemaRoot(transformedSchema)) {
        Assert.assertSame(transformedRoot, transformer.transform(clonedRoot, transformedRoot));
        Assert.assertEquals(transformedSchema, transformedRoot.getSchema());

        final int rowCount = originalRoot.getRowCount();
        Assert.assertEquals(rowCount, transformedRoot.getRowCount());

        final VarBinaryVector originalField1 =
            (VarBinaryVector) originalRoot.getVector("FIELD_1");
        final VarBinaryVector originalField2 =
            (VarBinaryVector) originalRoot.getVector("FIELD_2");
        final VarBinaryVector originalField3 =
            (VarBinaryVector) originalRoot.getVector("FIELD_3");

        final VarBinaryVector transformedField1 =
            (VarBinaryVector) transformedRoot.getVector("FIELD_1_RENAMED");
        final VarBinaryVector transformedField2 =
            (VarBinaryVector) transformedRoot.getVector("FIELD_2_RENAMED");
        final VarBinaryVector transformedField3 =
            (VarBinaryVector) transformedRoot.getVector("FIELD_3_RENAMED");
        final FieldVector emptyField = transformedRoot.getVector("EMPTY_FIELD");

        for (int i = 0; i < rowCount; i++) {
          Assert.assertArrayEquals(originalField1.getObject(i), transformedField1.getObject(i));
          Assert.assertArrayEquals(originalField2.getObject(i), transformedField2.getObject(i));
          Assert.assertArrayEquals(originalField3.getObject(i), transformedField3.getObject(i));
          Assert.assertNull(emptyField.getObject(i));
        }
      }
    }
  }

  private VectorSchemaRoot cloneVectorSchemaRoot(final VectorSchemaRoot originalRoot) {
    final VectorUnloader vectorUnloader = new VectorUnloader(originalRoot);
    try (final ArrowRecordBatch recordBatch = vectorUnloader.getRecordBatch()) {
      final VectorSchemaRoot clonedRoot = createVectorSchemaRoot(originalRoot.getSchema());
      final VectorLoader vectorLoader = new VectorLoader(clonedRoot);
      vectorLoader.load(recordBatch);
      return clonedRoot;
    }
  }

  private VectorSchemaRoot createVectorSchemaRoot(final Schema schema) {
    final List<FieldVector> fieldVectors = schema.getFields().stream()
        .map(field -> field.createVector(rootAllocator))
        .collect(Collectors.toList());
    return new VectorSchemaRoot(fieldVectors);
  }
}

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.TreeSet;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Test cases for {@link DictionaryUtils}.
 */
public class TestDictionaryUtils {

  @Test
  public void testReuseSchema() {
    FieldType varcharType = new FieldType(true, new ArrowType.Utf8(), null);
    FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);

    ImmutableList<Field> build = ImmutableList.of(
            new Field("stringCol", varcharType, null),
            new Field("intCol", intType, null));

    Schema schema = new Schema(build);
    Schema newSchema = DictionaryUtils.generateSchema(schema, null, new TreeSet<>());

    // assert that no new schema is created.
    assertTrue(schema == newSchema);
  }

  @Test
  public void testCreateSchema() {
    try (BufferAllocator allocator = new RootAllocator(1024)) {
      DictionaryEncoding dictionaryEncoding =
              new DictionaryEncoding(0, true, new ArrowType.Int(8, true));
      VarCharVector dictVec = new VarCharVector("dict vector", allocator);
      Dictionary dictionary = new Dictionary(dictVec, dictionaryEncoding);
      DictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider(dictionary);
      TreeSet<Long> dictionaryUsed = new TreeSet<>();

      FieldType encodedVarcharType = new FieldType(true, new ArrowType.Int(8, true), dictionaryEncoding);
      FieldType intType = new FieldType(true, new ArrowType.Int(32, true), null);

      ImmutableList<Field> build = ImmutableList.of(
              new Field("stringCol", encodedVarcharType, null),
              new Field("intCol", intType, null));

      Schema schema = new Schema(build);
      Schema newSchema = DictionaryUtils.generateSchema(schema, dictProvider, dictionaryUsed);

      // assert that a new schema is created.
      assertTrue(schema != newSchema);

      // assert the column is converted as expected
      ArrowType newColType = newSchema.getFields().get(0).getType();
      assertEquals(new ArrowType.Utf8(), newColType);

      assertEquals(1, dictionaryUsed.size());
      assertEquals(0, dictionaryUsed.first());
    }
  }
}

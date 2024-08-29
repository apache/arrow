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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;

import org.apache.arrow.c.Flags;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Test;

public class FlagsTest {
  @Test
  public void testForFieldNullableOrderedDict() {
    FieldType fieldType = new FieldType(true, ArrowType.Binary.INSTANCE,
        new DictionaryEncoding(123L, true, new ArrowType.Int(8, true)));

    assertEquals(Flags.ARROW_FLAG_DICTIONARY_ORDERED | Flags.ARROW_FLAG_NULLABLE,
        Flags.forField(new Field("Name", fieldType, new ArrayList<>())));
  }

  @Test
  public void testForFieldOrderedDict() {
    FieldType fieldType = new FieldType(false, ArrowType.Binary.INSTANCE,
        new DictionaryEncoding(123L, true, new ArrowType.Int(8, true)));
    assertEquals(Flags.ARROW_FLAG_DICTIONARY_ORDERED, Flags.forField(new Field("Name", fieldType, new ArrayList<>())));
  }

  @Test
  public void testForFieldNullableDict() {
    FieldType fieldType = new FieldType(true, ArrowType.Binary.INSTANCE,
        new DictionaryEncoding(123L, false, new ArrowType.Int(8, true)));
    assertEquals(Flags.ARROW_FLAG_NULLABLE, Flags.forField(new Field("Name", fieldType, new ArrayList<>())));
  }

  @Test
  public void testForFieldNullable() {
    FieldType fieldType = new FieldType(true, ArrowType.Binary.INSTANCE, null);
    assertEquals(Flags.ARROW_FLAG_NULLABLE, Flags.forField(new Field("Name", fieldType, new ArrayList<>())));
  }

  @Test
  public void testForFieldNullableOrderedSortedMap() {
    ArrowType.Map type = new ArrowType.Map(true);
    FieldType fieldType = new FieldType(true, type, new DictionaryEncoding(123L, true, new ArrowType.Int(8, true)));
    assertEquals(Flags.ARROW_FLAG_DICTIONARY_ORDERED | Flags.ARROW_FLAG_NULLABLE | Flags.ARROW_FLAG_MAP_KEYS_SORTED,
        Flags.forField(new Field("Name", fieldType, new ArrayList<>())));
  }

  @Test
  public void testForFieldNullableOrderedMap() {
    ArrowType.Map type = new ArrowType.Map(false);
    FieldType fieldType = new FieldType(true, type, new DictionaryEncoding(123L, true, new ArrowType.Int(8, true)));
    assertEquals(Flags.ARROW_FLAG_DICTIONARY_ORDERED | Flags.ARROW_FLAG_NULLABLE,
        Flags.forField(new Field("Name", fieldType, new ArrayList<>())));
  }
}

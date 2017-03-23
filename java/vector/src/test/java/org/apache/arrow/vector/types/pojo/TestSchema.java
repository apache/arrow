/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.vector.types.pojo;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.junit.Test;

public class TestSchema {

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, nullable, type, asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  @Test
  public void testComplex() throws IOException {
    Schema schema = new Schema(asList(
        field("a", false, new ArrowType.Int(8, true)),
        field("b", new ArrowType.Struct(),
            field("c", new ArrowType.Int(16, true)),
            field("d", new ArrowType.Utf8())),
        field("e", new ArrowType.List(), field(null, new ArrowType.Date())),
        field("f", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("g", new ArrowType.Timestamp(TimeUnit.MILLISECOND)),
        field("h", new ArrowType.Interval(IntervalUnit.DAY_TIME))
        ));
    roundTrip(schema);
    assertEquals(
        "Schema<a: Int(8, true) not null, b: Struct<c: Int(16, true), d: Utf8>, e: List<Date>, f: FloatingPoint(SINGLE), g: Timestamp(MILLISECOND), h: Interval(DAY_TIME)>",
        schema.toString());
  }

  @Test
  public void testAll() throws IOException {
    Schema schema = new Schema(asList(
        field("a", false, new ArrowType.Null()),
        field("b", new ArrowType.Struct(), field("ba", new ArrowType.Null())),
        field("c", new ArrowType.List(), field("ca", new ArrowType.Null())),
        field("d", new ArrowType.Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new ArrowType.Null())),
        field("e", new ArrowType.Int(8, true)),
        field("f", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("g", new ArrowType.Utf8()),
        field("h", new ArrowType.Binary()),
        field("i", new ArrowType.Bool()),
        field("j", new ArrowType.Decimal(5, 5)),
        field("k", new ArrowType.Date()),
        field("l", new ArrowType.Time(TimeUnit.MILLISECOND, 32)),
        field("m", new ArrowType.Timestamp(TimeUnit.MILLISECOND)),
        field("n", new ArrowType.Interval(IntervalUnit.DAY_TIME))
        ));
    roundTrip(schema);
  }

  @Test
  public void testUnion() throws IOException {
    Schema schema = new Schema(asList(
        field("d", new ArrowType.Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new ArrowType.Null()))
        ));
    roundTrip(schema);
    contains(schema, "Sparse");
  }

  @Test
  public void testTS() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new ArrowType.Timestamp(TimeUnit.SECOND)),
        field("b", new ArrowType.Timestamp(TimeUnit.MILLISECOND)),
        field("c", new ArrowType.Timestamp(TimeUnit.MICROSECOND)),
        field("d", new ArrowType.Timestamp(TimeUnit.NANOSECOND))
        ));
    roundTrip(schema);
    contains(schema, "SECOND", "MILLISECOND", "MICROSECOND", "NANOSECOND");
  }

  @Test
  public void testInterval() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new ArrowType.Interval(IntervalUnit.YEAR_MONTH)),
        field("b", new ArrowType.Interval(IntervalUnit.DAY_TIME))
        ));
    roundTrip(schema);
    contains(schema, "YEAR_MONTH", "DAY_TIME");
  }

  @Test
  public void testFP() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
        field("b", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("c", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))
        ));
    roundTrip(schema);
    contains(schema, "HALF", "SINGLE", "DOUBLE");
  }

  private void roundTrip(Schema schema) throws IOException {
    String json = schema.toJson();
    Schema actual = Schema.fromJSON(json);
    assertEquals(schema.toJson(), actual.toJson());
    assertEquals(schema, actual);
    validateFieldsHashcode(schema.getFields(), actual.getFields());
    assertEquals(schema.hashCode(), actual.hashCode());
  }

  private void validateFieldsHashcode(List<Field> schemaFields, List<Field> actualFields) {
    assertEquals(schemaFields.size(), actualFields.size());
    if (schemaFields.size() == 0) {
      return;
    }
    for (int i = 0; i < schemaFields.size(); i++) {
      Field schemaField = schemaFields.get(i);
      Field actualField = actualFields.get(i);
      validateFieldsHashcode(schemaField.getChildren(), actualField.getChildren());
      validateHashCode(schemaField.getType(), actualField.getType());
      validateHashCode(schemaField, actualField);
    }
  }

  private void validateHashCode(Object o1, Object o2) {
    assertEquals(o1, o2);
    assertEquals(o1 + " == " + o2, o1.hashCode(), o2.hashCode());
  }

  private void contains(Schema schema, String... s) throws IOException {
    String json = schema.toJson();
    for (String string : s) {
      assertTrue(json + " contains " + string, json.contains(string));
    }
  }

}

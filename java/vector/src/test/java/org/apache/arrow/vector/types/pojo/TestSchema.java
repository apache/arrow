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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.Decimal;
import org.apache.arrow.vector.types.pojo.ArrowType.Duration;
import org.apache.arrow.vector.types.pojo.ArrowType.FixedSizeBinary;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Interval;
import org.apache.arrow.vector.types.pojo.ArrowType.List;
import org.apache.arrow.vector.types.pojo.ArrowType.Null;
import org.apache.arrow.vector.types.pojo.ArrowType.Struct;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Union;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.junit.Test;

public class TestSchema {

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, new FieldType(nullable, type, null, null), asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  @Test
  public void testComplex() throws IOException {
    Schema schema = new Schema(asList(
        field("a", false, new Int(8, true)),
        field("b", new Struct(),
            field("c", new Int(16, true)),
            field("d", new Utf8())),
        field("e", new List(), field(null, new Date(DateUnit.MILLISECOND))),
        field("f", new FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("g", new Timestamp(TimeUnit.MILLISECOND, "UTC")),
        field("h", new Timestamp(TimeUnit.MICROSECOND, null)),
        field("i", new Interval(IntervalUnit.DAY_TIME)),
        field("j", new ArrowType.Duration(TimeUnit.SECOND))
    ));
    roundTrip(schema);
    assertEquals(
        "Schema<a: Int(8, true) not null, b: Struct<c: Int(16, true), d: Utf8>, e: List<Date(MILLISECOND)>, " +
          "f: FloatingPoint(SINGLE), g: Timestamp(MILLISECOND, UTC), h: Timestamp(MICROSECOND, null), " +
          "i: Interval(DAY_TIME), j: Duration(SECOND)>",
        schema.toString());
  }

  @Test
  public void testAll() throws IOException {
    Schema schema = new Schema(asList(
        field("a", false, new Null()),
        field("b", new Struct(), field("ba", new Null())),
        field("c", new List(), field("ca", new Null())),
        field("d", new Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new Null())),
        field("e", new Int(8, true)),
        field("f", new FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("g", new Utf8()),
        field("h", new Binary()),
        field("i", new Bool()),
        field("j", new Decimal(5, 5)),
        field("k", new Date(DateUnit.DAY)),
        field("l", new Date(DateUnit.MILLISECOND)),
        field("m", new Time(TimeUnit.SECOND, 32)),
        field("n", new Time(TimeUnit.MILLISECOND, 32)),
        field("o", new Time(TimeUnit.MICROSECOND, 64)),
        field("p", new Time(TimeUnit.NANOSECOND, 64)),
        field("q", new Timestamp(TimeUnit.MILLISECOND, "UTC")),
        field("r", new Timestamp(TimeUnit.MICROSECOND, null)),
        field("s", new Interval(IntervalUnit.DAY_TIME)),
        field("t", new FixedSizeBinary(100)),
        field("u", new Duration(TimeUnit.SECOND)),
        field("v", new Duration(TimeUnit.MICROSECOND))
    ));
    roundTrip(schema);
  }

  @Test
  public void testUnion() throws IOException {
    Schema schema = new Schema(asList(
        field("d", new Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new Null()))
    ));
    roundTrip(schema);
    contains(schema, "Sparse");
  }

  @Test
  public void testDate() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new Date(DateUnit.DAY)),
        field("b", new Date(DateUnit.MILLISECOND))
    ));
    roundTrip(schema);
    assertEquals(
        "Schema<a: Date(DAY), b: Date(MILLISECOND)>",
        schema.toString());
  }

  @Test
  public void testTime() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new Time(TimeUnit.SECOND, 32)),
        field("b", new Time(TimeUnit.MILLISECOND, 32)),
        field("c", new Time(TimeUnit.MICROSECOND, 64)),
        field("d", new Time(TimeUnit.NANOSECOND, 64))
    ));
    roundTrip(schema);
    assertEquals(
        "Schema<a: Time(SECOND, 32), b: Time(MILLISECOND, 32), c: Time(MICROSECOND, 64), d: Time(NANOSECOND, 64)>",
        schema.toString());
  }

  @Test
  public void testTS() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new Timestamp(TimeUnit.SECOND, "UTC")),
        field("b", new Timestamp(TimeUnit.MILLISECOND, "UTC")),
        field("c", new Timestamp(TimeUnit.MICROSECOND, "UTC")),
        field("d", new Timestamp(TimeUnit.NANOSECOND, "UTC")),
        field("e", new Timestamp(TimeUnit.SECOND, null)),
        field("f", new Timestamp(TimeUnit.MILLISECOND, null)),
        field("g", new Timestamp(TimeUnit.MICROSECOND, null)),
        field("h", new Timestamp(TimeUnit.NANOSECOND, null))
    ));
    roundTrip(schema);
    assertEquals(
        "Schema<a: Timestamp(SECOND, UTC), b: Timestamp(MILLISECOND, UTC), c: Timestamp(MICROSECOND, UTC), " +
          "d: Timestamp(NANOSECOND, UTC), e: Timestamp(SECOND, null), f: Timestamp(MILLISECOND, null), " +
          "g: Timestamp(MICROSECOND, null), h: Timestamp(NANOSECOND, null)>",
        schema.toString());
  }

  @Test
  public void testInterval() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new Interval(IntervalUnit.YEAR_MONTH)),
        field("b", new Interval(IntervalUnit.DAY_TIME))
    ));
    roundTrip(schema);
    contains(schema, "YEAR_MONTH", "DAY_TIME");
  }

  @Test
  public void testRoundTripDurationInterval() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new Duration(TimeUnit.SECOND)),
        field("b", new Duration(TimeUnit.MILLISECOND)),
        field("c", new Duration(TimeUnit.MICROSECOND)),
        field("d", new Duration(TimeUnit.NANOSECOND))
    ));
    roundTrip(schema);
    contains(schema, "SECOND", "MILLI", "MICRO", "NANO");
  }

  @Test
  public void testFP() throws IOException {
    Schema schema = new Schema(asList(
        field("a", new FloatingPoint(FloatingPointPrecision.HALF)),
        field("b", new FloatingPoint(FloatingPointPrecision.SINGLE)),
        field("c", new FloatingPoint(FloatingPointPrecision.DOUBLE))
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

  private void validateFieldsHashcode(java.util.List<Field> schemaFields, java.util.List<Field> actualFields) {
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

  private void contains(Schema schema, String... s) {
    String json = schema.toJson();
    for (String string : s) {
      assertTrue(json + " contains " + string, json.contains(string));
    }
  }

}

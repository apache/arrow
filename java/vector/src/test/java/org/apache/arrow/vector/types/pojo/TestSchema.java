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

import org.apache.arrow.flatbuf.IntervalUnit;
import org.apache.arrow.flatbuf.Precision;
import org.apache.arrow.flatbuf.TimeUnit;
import org.apache.arrow.flatbuf.UnionMode;
import org.junit.Test;

public class TestSchema {

  private static Field field(String name, boolean nullable, ArrowType type, Field... children) {
    return new Field(name, nullable, type, asList(children));
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return field(name, true, type, children);
  }

  @Test
  public void testAll() throws IOException {
    Schema schema = new Schema(asList(
        field("a", false, new ArrowType.Null()),
        field("b", new ArrowType.Struct_(), field("ba", new ArrowType.Null())),
        field("c", new ArrowType.List(), field("ca", new ArrowType.Null())),
        field("d", new ArrowType.Union(UnionMode.Sparse, new int[] {1, 2, 3}), field("da", new ArrowType.Null())),
        field("e", new ArrowType.Int(8, true)),
        field("f", new ArrowType.FloatingPoint(Precision.SINGLE)),
        field("g", new ArrowType.Utf8()),
        field("h", new ArrowType.Binary()),
        field("i", new ArrowType.Bool()),
        field("j", new ArrowType.Decimal(5, 5)),
        field("k", new ArrowType.Date()),
        field("l", new ArrowType.Time()),
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
        field("a", new ArrowType.FloatingPoint(Precision.HALF)),
        field("b", new ArrowType.FloatingPoint(Precision.SINGLE)),
        field("c", new ArrowType.FloatingPoint(Precision.DOUBLE))
        ));
    roundTrip(schema);
    contains(schema, "HALF", "SINGLE", "DOUBLE");
  }

  private void roundTrip(Schema schema) throws IOException {
    String json = schema.toJson();
    Schema actual = Schema.fromJSON(json);
    assertEquals(schema.toJson(), actual.toJson());
    assertEquals(schema, actual);
  }

  private void contains(Schema schema, String... s) throws IOException {
    String json = schema.toJson();
    for (String string : s) {
      assertTrue(json + " contains " + string, json.contains(string));
    }
  }

}

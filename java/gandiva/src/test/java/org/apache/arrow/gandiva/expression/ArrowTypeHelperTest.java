/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.gandiva.expression;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.ipc.GandivaTypes;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ArrowTypeHelperTest {

  private void testInt(int width, boolean isSigned, int expected) throws GandivaException {
    ArrowType arrowType = new ArrowType.Int(width, isSigned);
    GandivaTypes.ExtGandivaType gandivaType = ArrowTypeHelper.arrowTypeToProtobuf(arrowType);
    assertEquals(expected, gandivaType.getType().getNumber());
  }

  @Test
  public void testAllInts() throws GandivaException {
    testInt(8, false, GandivaTypes.GandivaType.UINT8_VALUE);
    testInt(8, true, GandivaTypes.GandivaType.INT8_VALUE);
    testInt(16, false, GandivaTypes.GandivaType.UINT16_VALUE);
    testInt(16, true, GandivaTypes.GandivaType.INT16_VALUE);
    testInt(32, false, GandivaTypes.GandivaType.UINT32_VALUE);
    testInt(32, true, GandivaTypes.GandivaType.INT32_VALUE);
    testInt(64, false, GandivaTypes.GandivaType.UINT64_VALUE);
    testInt(64, true, GandivaTypes.GandivaType.INT64_VALUE);
  }

  private void testFloat(FloatingPointPrecision precision, int expected) throws GandivaException {
    ArrowType arrowType = new ArrowType.FloatingPoint(precision);
    GandivaTypes.ExtGandivaType gandivaType = ArrowTypeHelper.arrowTypeToProtobuf(arrowType);
    assertEquals(expected, gandivaType.getType().getNumber());
  }

  @Test
  public void testAllFloats() throws GandivaException {
    testFloat(FloatingPointPrecision.HALF, GandivaTypes.GandivaType.HALF_FLOAT_VALUE);
    testFloat(FloatingPointPrecision.SINGLE, GandivaTypes.GandivaType.FLOAT_VALUE);
    testFloat(FloatingPointPrecision.DOUBLE, GandivaTypes.GandivaType.DOUBLE_VALUE);
  }

  private void testBasic(ArrowType arrowType, int expected) throws GandivaException {
    GandivaTypes.ExtGandivaType gandivaType = ArrowTypeHelper.arrowTypeToProtobuf(arrowType);
    assertEquals(expected, gandivaType.getType().getNumber());
  }

  @Test
  public void testSimpleTypes() throws GandivaException {
    testBasic(new ArrowType.Bool(), GandivaTypes.GandivaType.BOOL_VALUE);
    testBasic(new ArrowType.Binary(), GandivaTypes.GandivaType.BINARY_VALUE);
    testBasic(new ArrowType.Utf8(), GandivaTypes.GandivaType.UTF8_VALUE);
  }

  @Test
  public void testField() throws GandivaException {
    Field field = Field.nullable("col1", new ArrowType.Bool());
    GandivaTypes.Field f = ArrowTypeHelper.arrowFieldToProtobuf(field);
    assertEquals(field.getName(), f.getName());
    assertEquals(true, f.getNullable());
    assertEquals(GandivaTypes.GandivaType.BOOL_VALUE, f.getType().getType().getNumber());
  }

  @Test
  public void testSchema() throws GandivaException {
    Field a = Field.nullable("a", new ArrowType.Int(16, false));
    Field b = Field.nullable("b", new ArrowType.Int(32, true));
    Field c = Field.nullable("c", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));

    List<Field> fields = new ArrayList<Field>();
    fields.add(a);
    fields.add(b);
    fields.add(c);

    GandivaTypes.Schema schema = ArrowTypeHelper.arrowSchemaToProtobuf(new Schema(fields));
    int idx = 0;
    for (GandivaTypes.Field f : schema.getColumnsList()) {
      assertEquals(fields.get(idx).getName(), f.getName());
      idx++;
    }
  }
}


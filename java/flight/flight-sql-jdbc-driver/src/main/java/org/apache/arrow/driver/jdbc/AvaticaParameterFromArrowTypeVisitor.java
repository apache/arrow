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

package org.apache.arrow.driver.jdbc;

import java.sql.Types;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.avatica.AvaticaParameter;

/**
 * Turn an Arrow Field into an equivalent AvaticaParameter.
 */
class AvaticaParameterFromArrowTypeVisitor implements ArrowType.ArrowTypeVisitor<AvaticaParameter> {
  private final Field field;

  AvaticaParameterFromArrowTypeVisitor(Field field) {
    this.field = field;
  }

  @Override
  public AvaticaParameter visit(ArrowType.Null type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Struct type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.List type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.LargeList type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.FixedSizeList type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Union type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Map type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Int type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.FloatingPoint type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Utf8 type) {
    return new AvaticaParameter(/*signed*/false, /*precision*/0, /*scale*/0, Types.VARCHAR, "VARCHAR",
        String.class.getName(),
        field.getName());
  }

  @Override
  public AvaticaParameter visit(ArrowType.LargeUtf8 type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Binary type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.LargeBinary type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.FixedSizeBinary type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Bool type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Decimal type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Date type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Time type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Timestamp type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Interval type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  @Override
  public AvaticaParameter visit(ArrowType.Duration type) {
    throw new UnsupportedOperationException("Creating parameter with Arrow type " + type);
  }

  static AvaticaParameter fromArrowField(Field field) {
    return field.getType().accept(new AvaticaParameterFromArrowTypeVisitor(field));
  }
}

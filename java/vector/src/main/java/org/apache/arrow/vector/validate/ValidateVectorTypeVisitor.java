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

package org.apache.arrow.vector.validate;

import static org.apache.arrow.vector.validate.ValidateUtil.validateOrThrow;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseLargeVariableWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.IntervalDayVector;
import org.apache.arrow.vector.IntervalMonthDayNanoVector;
import org.apache.arrow.vector.IntervalYearVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.NonNullableStructVector;
import org.apache.arrow.vector.complex.UnionVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * Utility to validate vector type information.
 */
public class ValidateVectorTypeVisitor implements VectorVisitor<Void, Void> {

  private void validateVectorCommon(ValueVector vector, Class<? extends ArrowType> expectedArrowType) {
    validateOrThrow(vector.getField() != null, "Vector field is empty.");
    validateOrThrow(vector.getField().getFieldType() != null, "Vector field type is empty.");
    ArrowType arrowType = vector.getField().getFieldType().getType();
    validateOrThrow(arrowType != null, "Vector arrow type is empty.");
    validateOrThrow(expectedArrowType == arrowType.getClass(),
        "Incorrect arrow type for " + vector.getClass() + " : " + arrowType.toString());
  }

  private void validateIntVector(ValueVector vector, int expectedWidth, boolean expectedSigned) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.Int,
        "Vector %s is not an integer vector.", vector.getClass());
    ArrowType.Int intType = (ArrowType.Int) vector.getField().getFieldType().getType();
    validateOrThrow(intType.getIsSigned() == expectedSigned,
        "Expecting bit width %s, actual width %s.", expectedWidth, intType.getBitWidth());
    validateOrThrow(intType.getBitWidth() == expectedWidth, "Expecting bit width %s, actual bit width %s.",
        expectedWidth, intType.getBitWidth());
  }

  private void validateFloatingPointVector(ValueVector vector, FloatingPointPrecision expectedPrecision) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.FloatingPoint,
        "Vector %s is not a floating point vector.", vector.getClass());
    ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) vector.getField().getFieldType().getType();
    validateOrThrow(floatType.getPrecision() == expectedPrecision, "Expecting precision %s, actual precision %s.",
        expectedPrecision, floatType.getPrecision());
  }

  private void validateDateVector(ValueVector vector, DateUnit expectedDateUnit) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.Date,
        "Vector %s is not a date vector", vector.getClass());
    ArrowType.Date dateType = (ArrowType.Date) vector.getField().getFieldType().getType();
    validateOrThrow(dateType.getUnit() == expectedDateUnit,
        "Expecting date unit %s, actual date unit %s.", expectedDateUnit, dateType.getUnit());
  }

  private void validateDecimalVector(ValueVector vector) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.Decimal,
            "Vector %s is not a decimal vector", vector.getClass());
    ArrowType.Decimal decimalType = (ArrowType.Decimal) vector.getField().getFieldType().getType();
    validateOrThrow(decimalType.getScale() >= 0, "The scale of decimal %s is negative.", decimalType.getScale());
    validateOrThrow(decimalType.getScale() <= decimalType.getPrecision(),
            "The scale of decimal %s is greater than the precision %s.",
            decimalType.getScale(), decimalType.getPrecision());
    switch (decimalType.getBitWidth()) {
      case DecimalVector.TYPE_WIDTH * 8:
        validateOrThrow(decimalType.getPrecision() >= 1 && decimalType.getPrecision() <= DecimalVector.MAX_PRECISION,
                "Invalid precision %s for decimal 128.", decimalType.getPrecision());
        break;
      case Decimal256Vector.TYPE_WIDTH * 8:
        validateOrThrow(decimalType.getPrecision() >= 1 && decimalType.getPrecision() <= Decimal256Vector.MAX_PRECISION,
                "Invalid precision %s for decimal 256.", decimalType.getPrecision());
        break;
      default:
        throw new ValidateUtil.ValidateException("Only decimal 128 or decimal 256 are supported for decimal types");
    }
  }

  private void validateTimeVector(ValueVector vector, TimeUnit expectedTimeUnit, int expectedBitWidth) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.Time,
        "Vector %s is not a time vector.", vector.getClass());
    ArrowType.Time timeType = (ArrowType.Time) vector.getField().getFieldType().getType();
    validateOrThrow(timeType.getUnit() == expectedTimeUnit,
        "Expecting time unit %s, actual time unit %s.", expectedTimeUnit, timeType.getUnit());
    validateOrThrow(timeType.getBitWidth() == expectedBitWidth,
        "Expecting bit width %s, actual bit width %s.", expectedBitWidth, timeType.getBitWidth());
  }

  private void validateIntervalVector(ValueVector vector, IntervalUnit expectedIntervalUnit) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.Interval,
        "Vector %s is not an interval vector.", vector.getClass());
    ArrowType.Interval intervalType = (ArrowType.Interval) vector.getField().getFieldType().getType();
    validateOrThrow(intervalType.getUnit() == expectedIntervalUnit,
        "Expecting interval unit %s, actual date unit %s.", expectedIntervalUnit, intervalType.getUnit());
  }

  private void validateTimeStampVector(ValueVector vector, TimeUnit expectedTimeUnit, boolean expectTZ) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.Timestamp,
        "Vector %s is not a time stamp vector.", vector.getClass());
    ArrowType.Timestamp timestampType = (ArrowType.Timestamp) vector.getField().getFieldType().getType();
    validateOrThrow(timestampType.getUnit() == expectedTimeUnit,
        "Expecting time stamp unit %s, actual time stamp unit %s.", expectedTimeUnit, timestampType.getUnit());
    if (expectTZ) {
      validateOrThrow(timestampType.getTimezone() != null, "The time zone should not be null");
    } else {
      validateOrThrow(timestampType.getTimezone() == null, "The time zone should be null");
    }
  }

  private void validateExtensionTypeVector(ExtensionTypeVector<?> vector) {
    validateOrThrow(vector.getField().getFieldType().getType() instanceof ArrowType.ExtensionType,
        "Vector %s is not an extension type vector.", vector.getClass());
    validateOrThrow(vector.getField().getMetadata().containsKey(ArrowType.ExtensionType.EXTENSION_METADATA_KEY_NAME),
            "Field %s does not have proper extension type metadata: %s",
            vector.getField().getName(),
            vector.getField().getMetadata());
    // Validate the storage vector type
    vector.getUnderlyingVector().accept(this, null);
  }

  @Override
  public Void visit(BaseFixedWidthVector vector, Void value) {
    if (vector instanceof TinyIntVector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 8, true);
    } else if (vector instanceof SmallIntVector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 16, true);
    } else if (vector instanceof IntVector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 32, true);
    } else if (vector instanceof BigIntVector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 64, true);
    } else if (vector instanceof UInt1Vector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 8, false);
    } else if (vector instanceof UInt2Vector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 16, false);
    } else if (vector instanceof UInt4Vector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 32, false);
    } else if (vector instanceof UInt8Vector) {
      validateVectorCommon(vector, ArrowType.Int.class);
      validateIntVector(vector, 64, false);
    } else if (vector instanceof BitVector) {
      validateVectorCommon(vector, ArrowType.Bool.class);
    } else if (vector instanceof DecimalVector || vector instanceof Decimal256Vector) {
      validateVectorCommon(vector, ArrowType.Decimal.class);
      validateDecimalVector(vector);
    } else if (vector instanceof DateDayVector) {
      validateVectorCommon(vector, ArrowType.Date.class);
      validateDateVector(vector, DateUnit.DAY);
    } else if (vector instanceof DateMilliVector) {
      validateVectorCommon(vector, ArrowType.Date.class);
      validateDateVector(vector, DateUnit.MILLISECOND);
    } else if (vector instanceof DurationVector) {
      validateVectorCommon(vector, ArrowType.Duration.class);
      ArrowType.Duration arrowType = (ArrowType.Duration) vector.getField().getType();
      validateOrThrow(((DurationVector) vector).getUnit() == arrowType.getUnit(),
          "Different duration time unit for vector and arrow type. Vector time unit %s, type time unit %s.",
          ((DurationVector) vector).getUnit(), arrowType.getUnit());
    } else if (vector instanceof Float4Vector) {
      validateVectorCommon(vector, ArrowType.FloatingPoint.class);
      validateFloatingPointVector(vector, FloatingPointPrecision.SINGLE);
    } else if (vector instanceof Float8Vector) {
      validateVectorCommon(vector, ArrowType.FloatingPoint.class);
      validateFloatingPointVector(vector, FloatingPointPrecision.DOUBLE);
    } else if (vector instanceof IntervalDayVector) {
      validateVectorCommon(vector, ArrowType.Interval.class);
      validateIntervalVector(vector, IntervalUnit.DAY_TIME);
    } else if (vector instanceof IntervalMonthDayNanoVector) {
      validateVectorCommon(vector, ArrowType.Interval.class);
      validateIntervalVector(vector, IntervalUnit.MONTH_DAY_NANO);
    } else if (vector instanceof IntervalYearVector) {
      validateVectorCommon(vector, ArrowType.Interval.class);
      validateIntervalVector(vector, IntervalUnit.YEAR_MONTH);
    } else if (vector instanceof TimeMicroVector) {
      validateVectorCommon(vector, ArrowType.Time.class);
      validateTimeVector(vector, TimeUnit.MICROSECOND, 64);
    } else if (vector instanceof TimeMilliVector) {
      validateVectorCommon(vector, ArrowType.Time.class);
      validateTimeVector(vector, TimeUnit.MILLISECOND, 32);
    } else if (vector instanceof TimeNanoVector) {
      validateVectorCommon(vector, ArrowType.Time.class);
      validateTimeVector(vector, TimeUnit.NANOSECOND, 64);
    } else if (vector instanceof TimeSecVector) {
      validateVectorCommon(vector, ArrowType.Time.class);
      validateTimeVector(vector, TimeUnit.SECOND, 32);
    } else if (vector instanceof TimeStampMicroTZVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.MICROSECOND, true);
    } else if (vector instanceof TimeStampMicroVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.MICROSECOND, false);
    } else if (vector instanceof TimeStampMilliTZVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.MILLISECOND, true);
    } else if (vector instanceof TimeStampMilliVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.MILLISECOND, false);
    } else if (vector instanceof TimeStampNanoTZVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.NANOSECOND, true);
    } else if (vector instanceof TimeStampNanoVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.NANOSECOND, false);
    } else if (vector instanceof TimeStampSecTZVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.SECOND, true);
    } else if (vector instanceof TimeStampSecVector) {
      validateVectorCommon(vector, ArrowType.Timestamp.class);
      validateTimeStampVector(vector, TimeUnit.SECOND, false);
    } else if (vector instanceof FixedSizeBinaryVector) {
      validateVectorCommon(vector, ArrowType.FixedSizeBinary.class);
      ArrowType.FixedSizeBinary arrowType = (ArrowType.FixedSizeBinary) vector.getField().getType();
      validateOrThrow(arrowType.getByteWidth() > 0, "The byte width of a FixedSizeBinaryVector %s is not positive.",
          arrowType.getByteWidth());
      validateOrThrow(arrowType.getByteWidth() == vector.getTypeWidth(),
          "Type width mismatch for FixedSizeBinaryVector. Vector type width %s, arrow type type width %s.",
          vector.getTypeWidth(), arrowType.getByteWidth());
    } else {
      throw new IllegalArgumentException("Unknown type for fixed width vector " + vector.getClass());
    }
    return null;
  }

  @Override
  public Void visit(BaseVariableWidthVector vector, Void value) {
    if (vector instanceof VarCharVector) {
      validateVectorCommon(vector, ArrowType.Utf8.class);
    } else if (vector instanceof VarBinaryVector) {
      validateVectorCommon(vector, ArrowType.Binary.class);
    }
    return null;
  }

  @Override
  public Void visit(BaseLargeVariableWidthVector vector, Void value) {
    if (vector instanceof LargeVarCharVector) {
      validateVectorCommon(vector, ArrowType.LargeUtf8.class);
    } else if (vector instanceof LargeVarBinaryVector) {
      validateVectorCommon(vector, ArrowType.LargeBinary.class);
    }
    return null;
  }

  @Override
  public Void visit(ListVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.List.class);
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(FixedSizeListVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.FixedSizeList.class);
    ArrowType.FixedSizeList arrowType = (ArrowType.FixedSizeList) vector.getField().getType();
    validateOrThrow(arrowType.getListSize() == vector.getListSize(),
        "Inconsistent list size for FixedSizeListVector. Vector list size %s, arrow type list size %s.",
        vector.getListSize(), arrowType.getListSize());
    validateOrThrow(arrowType.getListSize() > 0, "The list size %s is not positive.", arrowType.getListSize());
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(LargeListVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.LargeList.class);
    ValueVector innerVector = vector.getDataVector();
    if (innerVector != null) {
      innerVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NonNullableStructVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.Struct.class);
    validateOrThrow(vector.getField().getChildren().size() == vector.getChildrenFromFields().size(),
        "Child field count and child vector count mismatch. Vector child count %s, field child count %s",
        vector.getChildrenFromFields().size(), vector.getField().getChildren().size());
    for (int i = 0; i < vector.getChildrenFromFields().size(); i++) {
      ValueVector subVector = vector.getChildByOrdinal(i);
      FieldType subType = vector.getField().getChildren().get(i).getFieldType();

      validateOrThrow(subType.equals(subVector.getField().getFieldType()),
          "Struct vector's field type not equal to the child vector's field type. " +
              "Struct field type %s, sub-vector field type %s", subType, subVector.getField().getFieldType());
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(UnionVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.Union.class);
    ArrowType.Union arrowType = (ArrowType.Union) vector.getField().getType();
    validateOrThrow(arrowType.getMode() == UnionMode.Sparse, "The union mode of UnionVector must be sparse");
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(DenseUnionVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.Union.class);
    ArrowType.Union arrowType = (ArrowType.Union) vector.getField().getType();
    validateOrThrow(arrowType.getMode() == UnionMode.Dense, "The union mode of DenseUnionVector must be dense");
    for (ValueVector subVector : vector.getChildrenFromFields()) {
      subVector.accept(this, null);
    }
    return null;
  }

  @Override
  public Void visit(NullVector vector, Void value) {
    validateVectorCommon(vector, ArrowType.Null.class);
    return null;
  }

  @Override
  public Void visit(ExtensionTypeVector<?> vector, Void value) {
    validateExtensionTypeVector(vector);
    return null;
  }
}

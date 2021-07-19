package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.Array;
import java.util.function.IntSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.LargeListVector;

public class ArrowFlightJdbcLargeListVectorAccessor extends AbstractArrowFlightJdbcListVectorAccessor {

  private final LargeListVector vector;

  public ArrowFlightJdbcLargeListVectorAccessor(LargeListVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  public Array getArray() {
    int index = getCurrentRow();
    if (this.wasNull = vector.isNull(index)) {
      return null;
    }

    long start = vector.getOffsetBuffer().getLong((long) index * 8L);
    long end = vector.getOffsetBuffer().getLong(((long) index + 1L) * 8L);
    FieldVector dataVector = vector.getDataVector();

    long count = end - start;
    return new ArrowFlightJdbcArray(dataVector, start, count);
  }

  @Override
  public Object getObject() {
    return vector.getObject(getCurrentRow());
  }
}

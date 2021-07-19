package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.Array;
import java.util.function.IntSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;

public class ArrowFlightJdbcListVectorAccessor extends AbstractArrowFlightJdbcListVectorAccessor {

  private final ListVector vector;

  public ArrowFlightJdbcListVectorAccessor(ListVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  public Array getArray() {
    int index = getCurrentRow();
    if (this.wasNull = vector.isNull(index)) {
      return null;
    }

    int start = vector.getOffsetBuffer().getInt(index * 4L);
    int end = vector.getOffsetBuffer().getInt((index + 1) * 4L);
    FieldVector dataVector = vector.getDataVector();

    int count = end - start;
    return new ArrayImpl(dataVector, start, count);
  }

  @Override
  public Object getObject() {
    return vector.getObject(getCurrentRow());
  }
}

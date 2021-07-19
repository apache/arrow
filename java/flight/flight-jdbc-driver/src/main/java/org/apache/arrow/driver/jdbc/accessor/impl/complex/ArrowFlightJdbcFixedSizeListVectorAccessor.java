package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.Array;
import java.util.function.IntSupplier;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;

public class ArrowFlightJdbcFixedSizeListVectorAccessor extends AbstractArrowFlightJdbcListVectorAccessor {

  private final FixedSizeListVector vector;

  public ArrowFlightJdbcFixedSizeListVectorAccessor(FixedSizeListVector vector, IntSupplier currentRowSupplier) {
    super(currentRowSupplier);
    this.vector = vector;
  }

  @Override
  public Array getArray() {
    int index = getCurrentRow();
    int start = vector.getListSize() * index;
    int count = vector.getListSize();

    FieldVector dataVector = vector.getDataVector();
    return new ArrayImpl(dataVector, start, count);
  }

  @Override
  public Object getObject() {
    return vector.getObject(getCurrentRow());
  }
}

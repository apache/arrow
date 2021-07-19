package org.apache.arrow.driver.jdbc.accessor.impl.complex;

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
  protected long getStart(int index) {
    return (long) vector.getListSize() * index;
  }

  @Override
  protected long getEnd(int index) {
    return (long) vector.getListSize() * (index + 1);
  }

  @Override
  protected FieldVector getDataVector() {
    return vector.getDataVector();
  }

  @Override
  public Object getObject() {
    return vector.getObject(getCurrentRow());
  }
}

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
  protected long getStart(int index) {
    return vector.getOffsetBuffer().getInt(index * 4L);
  }

  @Override
  protected long getEnd(int index) {
    return vector.getOffsetBuffer().getInt((index + 1) * 4L);
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

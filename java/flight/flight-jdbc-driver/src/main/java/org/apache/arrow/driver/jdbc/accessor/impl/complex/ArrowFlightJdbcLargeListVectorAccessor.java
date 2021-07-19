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
  protected long getStart(int index) {
    return vector.getOffsetBuffer().getLong((long) index * 8L);
  }

  @Override
  protected long getEnd(int index) {
    return vector.getOffsetBuffer().getLong(((long) index + 1L) * 8L);
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

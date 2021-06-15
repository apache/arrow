package org.apache.arrow.driver.jdbc;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.Cursor;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

public class ArrowFlightJdbcCursor implements Cursor {
  @Override
  public List<Accessor> createAccessors(List<ColumnMetaData> list, Calendar calendar, ArrayImpl.Factory factory) {
    // TODO Fill this stub.
    return null;
  }

  @Override
  public boolean next() throws SQLException {
    // TODO Fill this stub.
    return false;
  }

  @Override
  public void close() {

  }

  @Override
  public boolean wasNull() throws SQLException {
    // TODO Fill this stub.
    return false;
  }
}

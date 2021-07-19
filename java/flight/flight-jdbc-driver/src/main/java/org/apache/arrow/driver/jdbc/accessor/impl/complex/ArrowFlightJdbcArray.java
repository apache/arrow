package org.apache.arrow.driver.jdbc.accessor.impl.complex;

import java.sql.Array;
import java.sql.ResultSet;
import java.util.Map;

public class ArrowFlightJdbcArray implements Array {

  @Override
  public String getBaseTypeName() {
    return null;
  }

  @Override
  public int getBaseType() {
    return 0;
  }

  @Override
  public Object getArray() {
    return null;
  }

  @Override
  public Object getArray(Map<String, Class<?>> map) {
    return null;
  }

  @Override
  public Object getArray(long l, int i) {
    return null;
  }

  @Override
  public Object getArray(long l, int i, Map<String, Class<?>> map) {
    return null;
  }

  @Override
  public ResultSet getResultSet() {
    return null;
  }

  @Override
  public ResultSet getResultSet(Map<String, Class<?>> map) {
    return null;
  }

  @Override
  public ResultSet getResultSet(long l, int i) {
    return null;
  }

  @Override
  public ResultSet getResultSet(long l, int i, Map<String, Class<?>> map) {
    return null;
  }

  @Override
  public void free() {

  }
}

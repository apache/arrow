package org.apache.arrow.driver.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.SQLException;
import java.util.Properties;

abstract class AbstractFactory implements AvaticaFactory {
  protected final int major;
  protected final int minor;

  public AbstractFactory(int major, int minor) {
    this.major = major;
    this.minor = minor;
  }

  @Override
  public AvaticaConnection newConnection(UnregisteredDriver driver,
                                         AvaticaFactory factory,
                                         String url, Properties info) throws SQLException {
    return newConnection((ArrowFlightJdbcDriver) driver, (AbstractFactory) factory, url, info);
  }

  abstract ArrowFlightConnection newConnection(ArrowFlightJdbcDriver driver,
                                               AbstractFactory factory,
                                               String url,
                                               Properties info) throws SQLException;
}

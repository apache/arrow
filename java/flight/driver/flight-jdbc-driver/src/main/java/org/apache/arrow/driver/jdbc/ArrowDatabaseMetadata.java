package org.apache.arrow.driver.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

public class ArrowDatabaseMetadata extends AvaticaDatabaseMetaData {
  
  protected ArrowDatabaseMetadata(AvaticaConnection connection) {
    super(connection);
  }
}

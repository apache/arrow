package org.apache.arrow.driver.jdbc;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.arrow.driver.jdbc.adhoc.MockFlightSqlProducer;
import org.junit.ClassRule;
import org.junit.Test;

public class FlightCookieTest {

  private static final MockFlightSqlProducer FLIGHT_SQL_PRODUCER = new MockFlightSqlProducer();
  @ClassRule
  public static final FlightServerTestRule FLIGHT_SERVER_TEST_RULE =
      FlightServerTestRule.createStandardTestRule(FLIGHT_SQL_PRODUCER);

  @Test
  public void testeCookies() throws SQLException {
    Connection connection = FLIGHT_SERVER_TEST_RULE.getConnection();
//    FlightServerTestRule.MiddlwareCookie.Factory factory = FLIGHT_SERVER_TEST_RULE.getFactory();
//    String cookie = factory.getCookie();
    ResultSet catalogs = connection.getMetaData().getSchemas();
//    String cookie1 = factory.getCookie();
//    System.out.println("Cookie: " + cookie + "Cookie1: " + cookie1);
  }


  }


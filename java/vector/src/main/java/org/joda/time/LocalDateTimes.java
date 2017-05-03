package org.joda.time;

/**
 * Workaround to access package protected fields in JODA
 *
 */
public class LocalDateTimes {

  public static long getLocalMillis(LocalDateTime localDateTime) {
    return localDateTime.getLocalMillis();
  }

}

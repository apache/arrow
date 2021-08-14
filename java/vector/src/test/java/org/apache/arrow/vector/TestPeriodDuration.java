package org.apache.arrow.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.time.Duration;
import java.time.Period;
import org.junit.Test;

public class TestPeriodDuration {

  @Test
  public void testBasics() {
    PeriodDuration pd1 = new PeriodDuration(Period.of(1, 2, 3), Duration.ofNanos(123));
    PeriodDuration pd1_eq = new PeriodDuration(Period.of(1, 2, 3), Duration.ofNanos(123));
    PeriodDuration pd2 = new PeriodDuration(Period.of(1, 2, 3), Duration.ofNanos(12));
    PeriodDuration pd3 = new PeriodDuration(Period.of(1, 2, 0), Duration.ofNanos(123));

    assertEquals(pd1, pd1_eq);
    assertEquals(pd1.hashCode(), pd1_eq.hashCode());

    assertNotEquals(pd1, pd2);
    assertNotEquals(pd1.hashCode(), pd2.hashCode());
    assertNotEquals(pd1, pd3);
    assertNotEquals(pd1.hashCode(), pd3.hashCode());
  }

}

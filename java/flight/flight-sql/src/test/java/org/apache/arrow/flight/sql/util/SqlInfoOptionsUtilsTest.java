package org.apache.arrow.flight.sql.util;

import static java.util.stream.Collectors.toCollection;
import static org.apache.arrow.flight.sql.util.SqlInfoOptionsUtils.doesBitmaskTranslateToEnum;
import static org.hamcrest.CoreMatchers.is;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.ProtocolMessageEnum;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public final class SqlInfoOptionsUtilsTest {

  @Parameter
  public long bitmask;
  @Parameter(value = 1)
  public Set<TestOption> messageEnums;
  public Set<TestOption> expectedOutcome;
  @Rule
  public final ErrorCollector collector = new ErrorCollector();

  @Before
  public void setUp() {
    expectedOutcome =
        Arrays.stream(TestOption.values())
            .filter(enumInstance -> doesBitmaskTranslateToEnum(enumInstance, bitmask))
            .collect(toCollection(() -> EnumSet.noneOf(TestOption.class)));
  }

  @Parameters
  public static List<Object[]> provideParameters() {
    return Arrays.asList(new Object[][]{
        {0, EnumSet.noneOf(TestOption.class)},
        {1, EnumSet.of(TestOption.OPTION_A)},
        {0b10, EnumSet.of(TestOption.OPTION_B)},
        {0b11, EnumSet.of(TestOption.OPTION_A, TestOption.OPTION_B)},
        {0b100, EnumSet.of(TestOption.OPTION_C)},
        {0b101, EnumSet.of(TestOption.OPTION_A, TestOption.OPTION_C)},
        {0b110, EnumSet.of(TestOption.OPTION_B, TestOption.OPTION_C)},
        {0b111, EnumSet.allOf(TestOption.class)},
    });
  }

  @Test
  public void testShouldFilterOutEnumsBasedOnBitmask() {
    collector.checkThat(messageEnums, is(expectedOutcome));
  }

  private enum TestOption implements ProtocolMessageEnum {
    OPTION_A, OPTION_B, OPTION_C;

    @Override
    public int getNumber() {
      return ordinal();
    }

    @Override
    public EnumValueDescriptor getValueDescriptor() {
      throw getUnsupportedException();
    }

    @Override
    public EnumDescriptor getDescriptorForType() {
      throw getUnsupportedException();
    }

    private UnsupportedOperationException getUnsupportedException() {
      return new UnsupportedOperationException("Unimplemented method is irrelevant for the scope of this test.");
    }
  }
}
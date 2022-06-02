

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDecimalVector {

  @Test
  public void testWriteBigEndian() {
    try (DecimalVector decimalVector = TestUtils.newVector(DecimalVector.class, "decimal",
        new ArrowType.Decimal(38, 9, 128), allocator);) {
      decimalVector.allocateNew();
      BigDecimal decimal1 = new BigDecimal("123456789.000000000");
      BigDecimal decimal2 = new BigDecimal("11.123456789");
      BigDecimal decimal3 = new BigDecimal("1.000000000");
      BigDecimal decimal4 = new BigDecimal("0.111111111");
      BigDecimal decimal5 = new BigDecimal("987654321.123456789");
      BigDecimal decimal6 = new BigDecimal("222222222222.222222222");
      BigDecimal decimal7 = new BigDecimal("7777777777777.666666667");
      BigDecimal decimal8 = new BigDecimal("1212121212.343434343");

      byte[] decimalValue1 = decimal1.unscaledValue().toByteArray();
      byte[] decimalValue2 = decimal2.unscaledValue().toByteArray();
      byte[] decimalValue3 = decimal3.unscaledValue().toByteArray();
      byte[] decimalValue4 = decimal4.unscaledValue().toByteArray();
      byte[] decimalValue5 = decimal5.unscaledValue().toByteArray();
      byte[] decimalValue6 = decimal6.unscaledValue().toByteArray();
      byte[] decimalValue7 = decimal7.unscaledValue().toByteArray();
      byte[] decimalValue8 = decimal8.unscaledValue().toByteArray();

      decimalVector.setBigEndian(0, decimalValue1);
      decimalVector.setBigEndian(1, decimalValue2);
      decimalVector.setBigEndian(2, decimalValue3);
      decimalVector.setBigEndian(3, decimalValue4);
      decimalVector.setBigEndian(4, decimalValue5);
      decimalVector.setBigEndian(5, decimalValue6);
      decimalVector.setBigEndian(6, decimalValue7);
      decimalVector.setBigEndian(7, decimalValue8);

      decimalVector.setValueCount(8);
      assertEquals(8, decimalVector.getValueCount());
      assertEquals(decimal1, decimalVector.getObject(0));
      assertEquals(decimal2, decimalVector.getObject(1));
      assertEquals(decimal3, decimalVector.getObject(2));
      assertEquals(decimal4, decimalVector.getObject(3));
      assertEquals(decimal5, decimalVector.getObject(4));
      assertEquals(decimal6, decimalVector.getObject(5));
      assertEquals(decimal7, decimalVector.getObject(6));
      assertEquals(decimal8, decimalVector.getObject(7));
    }
  }
} 

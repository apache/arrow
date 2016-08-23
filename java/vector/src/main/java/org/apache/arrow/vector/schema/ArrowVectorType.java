package org.apache.arrow.vector.schema;

import org.apache.arrow.flatbuf.VectorType;

public class ArrowVectorType {

  public static final ArrowVectorType VALUES = new ArrowVectorType(VectorType.VALUES);
  public static final ArrowVectorType OFFSET = new ArrowVectorType(VectorType.OFFSET);
  public static final ArrowVectorType VALIDITY = new ArrowVectorType(VectorType.VALIDITY);
  public static final ArrowVectorType TYPE = new ArrowVectorType(VectorType.TYPE);

  private final short type;

  public ArrowVectorType(short type) {
    this.type = type;
  }

  public short getType() {
    return type;
  }

  @Override
  public String toString() {
    try {
      return VectorType.name(type);
    } catch (ArrayIndexOutOfBoundsException e) {
      return "Unlnown type " + type;
    }
  }
}

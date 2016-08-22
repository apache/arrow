package org.apache.arrow.vector.layout;

public class VectorLayout {

  public static VectorLayout newOffsetVectorLayout() {
    return newIntVectorLayout(32);
  }

  public static VectorLayout newIntVectorLayout(int typeBitWidth) {
    switch (typeBitWidth) {
    case 8:
    case 16:
    case 32:
    case 64:
      return new VectorLayout(typeBitWidth);
    default:
      throw new IllegalArgumentException("only 8, 16, 32, or 64 bits supported");
    }
  }

  public static VectorLayout newBooleanVectorLayout() {
    return new VectorLayout(1);
  }

  public static VectorLayout newValidityVectorLayout() {
    return newBooleanVectorLayout();
  }

  public static VectorLayout newByteVectorLayout() {
    return newIntVectorLayout(8);
  }

  private final int typeBitWidth;

  public VectorLayout(int typeBitWidth) {
    super();
    this.typeBitWidth = typeBitWidth;
  }

  public int getTypeBitWidth() {
    return typeBitWidth;
  }

}

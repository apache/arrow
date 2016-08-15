package org.apache.arrow.vector.layout;

public class ByteAlignedVectorLayout extends VectorLayout {

  private final int typeByteWidth;

  public ByteAlignedVectorLayout(int typeByteWidth) {
    super(typeByteWidth * 8);
    this.typeByteWidth = typeByteWidth;
  }

  public int getTypeByteWidth() {
    return typeByteWidth;
  }

}

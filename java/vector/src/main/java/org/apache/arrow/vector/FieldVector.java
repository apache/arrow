package org.apache.arrow.vector;

import java.util.List;

import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.Field;

import io.netty.buffer.ArrowBuf;

public interface FieldVector extends ValueVector {

  /**
   * Initializes the child vectors
   * to be later loaded with loadBuffers
   * @param children
   */
  void initializeChildrenFromFields(List<Field> children);

  List<FieldVector> getChildrenFromFields();

  void loadFieldBuffers(ArrowFieldNode fieldNode, List<ArrowBuf> ownBuffers);

  /**
   * Returns the own buffers for this vector
   * @return the
   */
  List<ArrowBuf> getFieldBuffers();

}

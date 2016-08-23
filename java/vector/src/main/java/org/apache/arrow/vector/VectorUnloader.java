package org.apache.arrow.vector;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.ValueVector.Accessor;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

public class VectorUnloader {

  private final MapVector parent;

  public VectorUnloader(MapVector parent) {
    super();
    this.parent = parent;
  }

  public Schema getSchema() {
    Field rootField = parent.getField();
    return new Schema(rootField.getChildren());
  }

  public ArrowRecordBatch getRecordBatch() {
    List<ArrowFieldNode> nodes = new ArrayList<>();
    List<ArrowBuf> buffers = new ArrayList<>();
    for (FieldVector vector : parent.getFieldVectors()) {
      appendNodes(vector, nodes, buffers);
    }
    return new ArrowRecordBatch(parent.getAccessor().getValueCount(), nodes, buffers);
  }

  private void appendNodes(FieldVector vector, List<ArrowFieldNode> nodes, List<ArrowBuf> buffers) {
    Accessor accessor = vector.getAccessor();
    int nullCount = 0;
    // TODO: should not have to do that
    // we can do that a lot more efficiently (for example with Long.bitCount(i))
    for (int i = 0; i < accessor.getValueCount(); i++) {
      if (accessor.isNull(i)) {
        nullCount ++;
      }
    }
    nodes.add(new ArrowFieldNode(accessor.getValueCount(), nullCount));
    // TODO: validate buffer count
    buffers.addAll(vector.getFieldBuffers());
    for (FieldVector child : vector.getChildrenFromFields()) {
      appendNodes(child, nodes, buffers);
    }
  }
}

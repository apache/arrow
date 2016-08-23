package org.apache.arrow.vector;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.schema.ArrowFieldNode;
import org.apache.arrow.vector.schema.ArrowRecordBatch;
import org.apache.arrow.vector.schema.VectorLayout;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import io.netty.buffer.ArrowBuf;

public class VectorLoader {
  private final List<FieldVector> fieldVectors;
  private final List<Field> fields;

  public VectorLoader(Schema schema, MapVector root) {
    super();
    this.fields = schema.getFields();
    root.initializeChildren(fields);
    this.fieldVectors = root.getFieldVectors();
    if (this.fieldVectors.size() != fields.size()) {
      throw new IllegalArgumentException(); //TODO
    }
  }

  public void load(ArrowRecordBatch recordBatch) {
    Iterator<ArrowBuf> buffers = recordBatch.getBuffers().iterator();
    Iterator<ArrowFieldNode> nodes = recordBatch.getNodes().iterator();
    for (int i = 0; i < fields.size(); ++i) {
      Field field = fields.get(i);
      FieldVector fieldVector = fieldVectors.get(i);
      loadBuffers(fieldVector, field, buffers, nodes);
    }
  }

  private void loadBuffers(FieldVector vector, Field field, Iterator<ArrowBuf> buffers, Iterator<ArrowFieldNode> nodes) {
    ArrowFieldNode fieldNode = nodes.next();
    List<VectorLayout> typeLayout = field.getTypeLayout().getVectors();
    List<ArrowBuf> ownBuffers = new ArrayList<>(typeLayout.size());
    for (int j = 0; j < typeLayout.size(); j++) {
      ownBuffers.add(buffers.next());
    }
    vector.loadFieldBuffers(fieldNode, ownBuffers);
    List<Field> children = field.getChildren();
    if (children.size() > 0) {
      List<FieldVector> childrenFromFields = vector.getChildrenFromFields();
      int i = 0;
      checkArgument(children.size() == childrenFromFields.size(), "should have as many children as in the schema: found " + childrenFromFields.size() + " expected " + children.size());
      for (Field child : children) {
        FieldVector fieldVector = childrenFromFields.get(i);
        loadBuffers(fieldVector, child, buffers, nodes);
        ++i;
      }
    }
  }
}

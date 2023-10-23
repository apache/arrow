/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight.sql.util;

import static java.util.Objects.isNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.DenseUnionVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.util.Text;

public class FlightStreamUtils {

  public static List<List<String>> getResults(FlightStream stream) {
    final List<List<String>> results = new ArrayList<>();
    while (stream.next()) {
      try (final VectorSchemaRoot root = stream.getRoot()) {
        final long rowCount = root.getRowCount();
        for (int i = 0; i < rowCount; ++i) {
          results.add(new ArrayList<>());
        }

        root.getSchema().getFields().forEach(field -> {
          try (final FieldVector fieldVector = root.getVector(field.getName())) {
            if (fieldVector instanceof VarCharVector) {
              final VarCharVector varcharVector = (VarCharVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Text data = varcharVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : data.toString());
              }
            } else if (fieldVector instanceof IntVector) {
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object data = fieldVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof VarBinaryVector) {
              final VarBinaryVector varbinaryVector = (VarBinaryVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final byte[] data = varbinaryVector.getObject(rowIndex);
                final String output;
                try {
                  output = isNull(data) ?
                      null :
                      MessageSerializer.deserializeSchema(
                          new ReadChannel(Channels.newChannel(new ByteArrayInputStream(data)))).toJson();
                } catch (final IOException e) {
                  throw new RuntimeException("Failed to deserialize schema", e);
                }
                results.get(rowIndex).add(output);
              }
            } else if (fieldVector instanceof DenseUnionVector) {
              final DenseUnionVector denseUnionVector = (DenseUnionVector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = denseUnionVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof ListVector) {
              for (int i = 0; i < fieldVector.getValueCount(); i++) {
                if (!fieldVector.isNull(i)) {
                  List<Text> elements = (List<Text>) ((ListVector) fieldVector).getObject(i);
                  List<String> values = new ArrayList<>();

                  for (Text element : elements) {
                    values.add(element.toString());
                  }
                  results.get(i).add(values.toString());
                }
              }

            } else if (fieldVector instanceof UInt4Vector) {
              final UInt4Vector uInt4Vector = (UInt4Vector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = uInt4Vector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof UInt1Vector) {
              final UInt1Vector uInt1Vector = (UInt1Vector) fieldVector;
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                final Object data = uInt1Vector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else if (fieldVector instanceof BitVector) {
              for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                Object data = fieldVector.getObject(rowIndex);
                results.get(rowIndex).add(isNull(data) ? null : Objects.toString(data));
              }
            } else {
              throw new UnsupportedOperationException("Not yet implemented");
            }
          }
        });
      }
    }

    return results;
  }
}

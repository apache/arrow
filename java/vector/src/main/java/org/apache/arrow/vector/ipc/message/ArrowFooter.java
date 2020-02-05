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

package org.apache.arrow.vector.ipc.message;

import static org.apache.arrow.vector.ipc.message.FBSerializables.writeAllStructsToVector;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.flatbuf.Block;
import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.vector.types.pojo.Schema;

import com.google.flatbuffers.FlatBufferBuilder;

/** Footer metadata for the arrow file format. */
public class ArrowFooter implements FBSerializable {

  private final Schema schema;

  private final List<ArrowBlock> dictionaries;

  private final List<ArrowBlock> recordBatches;

  /**
   * Constructs a new instance.
   *
   * @param schema The schema for record batches in the file.
   * @param dictionaries  The dictionaries relevant to the file.
   * @param recordBatches  The recordBatches written to the file.
   */
  public ArrowFooter(Schema schema, List<ArrowBlock> dictionaries, List<ArrowBlock> recordBatches) {
    this.schema = schema;
    this.dictionaries = dictionaries;
    this.recordBatches = recordBatches;
  }

  /**
   * Constructs from the corresponding Flatbuffer message.
   */
  public ArrowFooter(Footer footer) {
    this(
        Schema.convertSchema(footer.schema()),
        dictionaries(footer),
        recordBatches(footer)
    );
  }

  private static List<ArrowBlock> recordBatches(Footer footer) {
    List<ArrowBlock> recordBatches = new ArrayList<>();
    Block tempBlock = new Block();
    int recordBatchesLength = footer.recordBatchesLength();
    for (int i = 0; i < recordBatchesLength; i++) {
      Block block = footer.recordBatches(tempBlock, i);
      recordBatches.add(new ArrowBlock(block.offset(), block.metaDataLength(), block.bodyLength()));
    }
    return recordBatches;
  }

  private static List<ArrowBlock> dictionaries(Footer footer) {
    List<ArrowBlock> dictionaries = new ArrayList<>();
    Block tempBlock = new Block();

    int dictionariesLength = footer.dictionariesLength();
    for (int i = 0; i < dictionariesLength; i++) {
      Block block = footer.dictionaries(tempBlock, i);
      dictionaries.add(new ArrowBlock(block.offset(), block.metaDataLength(), block.bodyLength()));
    }
    return dictionaries;
  }

  public Schema getSchema() {
    return schema;
  }

  public List<ArrowBlock> getDictionaries() {
    return dictionaries;
  }

  public List<ArrowBlock> getRecordBatches() {
    return recordBatches;
  }

  @Override
  public int writeTo(FlatBufferBuilder builder) {
    int schemaIndex = schema.getSchema(builder);
    Footer.startDictionariesVector(builder, dictionaries.size());
    int dicsOffset = writeAllStructsToVector(builder, dictionaries);
    Footer.startRecordBatchesVector(builder, recordBatches.size());
    int rbsOffset = writeAllStructsToVector(builder, recordBatches);
    Footer.startFooter(builder);
    Footer.addSchema(builder, schemaIndex);
    Footer.addDictionaries(builder, dicsOffset);
    Footer.addRecordBatches(builder, rbsOffset);
    return Footer.endFooter(builder);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dictionaries == null) ? 0 : dictionaries.hashCode());
    result = prime * result + ((recordBatches == null) ? 0 : recordBatches.hashCode());
    result = prime * result + ((schema == null) ? 0 : schema.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ArrowFooter other = (ArrowFooter) obj;
    if (dictionaries == null) {
      if (other.dictionaries != null) {
        return false;
      }
    } else if (!dictionaries.equals(other.dictionaries)) {
      return false;
    }
    if (recordBatches == null) {
      if (other.recordBatches != null) {
        return false;
      }
    } else if (!recordBatches.equals(other.recordBatches)) {
      return false;
    }
    if (schema == null) {
      if (other.schema != null) {
        return false;
      }
    } else if (!schema.equals(other.schema)) {
      return false;
    }
    return true;
  }
}

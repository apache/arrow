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

package org.apache.arrow.c;

import static org.apache.arrow.c.NativeUtil.NULL;
import static org.apache.arrow.c.NativeUtil.addressOrNull;
import static org.apache.arrow.util.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.c.jni.JniWrapper;
import org.apache.arrow.c.jni.PrivateData;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Exporter for {@link ArrowSchema}.
 */
final class SchemaExporter {
  private final BufferAllocator allocator;

  public SchemaExporter(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Private data structure for exported schemas.
   */
  static class ExportedSchemaPrivateData implements PrivateData {
    ArrowBuf format;
    ArrowBuf name;
    ArrowBuf metadata;
    ArrowBuf children_ptrs;
    ArrowSchema dictionary;
    List<ArrowSchema> children;

    @Override
    public void close() {
      NativeUtil.closeBuffer(format);
      NativeUtil.closeBuffer(name);
      NativeUtil.closeBuffer(metadata);
      NativeUtil.closeBuffer(children_ptrs);
      if (dictionary != null) {
        dictionary.close();
      }
      if (children != null) {
        for (ArrowSchema child : children) {
          child.close();
        }
      }
    }
  }

  void export(ArrowSchema schema, Field field, DictionaryProvider dictionaryProvider) {
    String name = field.getName();
    String format = Format.asString(field.getType());
    long flags = Flags.forField(field);
    List<Field> children = field.getChildren();
    DictionaryEncoding dictionaryEncoding = field.getDictionary();

    ExportedSchemaPrivateData data = new ExportedSchemaPrivateData();
    try {
      data.format = NativeUtil.toNativeString(allocator, format);
      data.name = NativeUtil.toNativeString(allocator, name);
      data.metadata = Metadata.encode(allocator, field.getMetadata());

      if (children != null) {
        data.children = new ArrayList<>(children.size());
        data.children_ptrs = allocator.buffer((long) children.size() * Long.BYTES);
        for (int i = 0; i < children.size(); i++) {
          ArrowSchema child = ArrowSchema.allocateNew(allocator);
          data.children.add(child);
          data.children_ptrs.writeLong(child.memoryAddress());
        }
      }

      if (dictionaryEncoding != null) {
        Dictionary dictionary = dictionaryProvider.lookup(dictionaryEncoding.getId());
        checkNotNull(dictionary, "Dictionary lookup failed on export of field with dictionary");

        data.dictionary = ArrowSchema.allocateNew(allocator);
        export(data.dictionary, dictionary.getVector().getField(), dictionaryProvider);
      }

      ArrowSchema.Snapshot snapshot = new ArrowSchema.Snapshot();
      snapshot.format = data.format.memoryAddress();
      snapshot.name = addressOrNull(data.name);
      snapshot.metadata = addressOrNull(data.metadata);
      snapshot.flags = flags;
      snapshot.n_children = (data.children != null) ? data.children.size() : 0;
      snapshot.children = addressOrNull(data.children_ptrs);
      snapshot.dictionary = addressOrNull(data.dictionary);
      snapshot.release = NULL;
      schema.save(snapshot);

      // sets release and private data
      JniWrapper.get().exportSchema(schema.memoryAddress(), data);
    } catch (Exception e) {
      data.close();
      throw e;
    }

    // Export children
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        Field childField = children.get(i);
        ArrowSchema child = data.children.get(i);
        export(child, childField, dictionaryProvider);
      }
    }
  }
}

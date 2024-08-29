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
import static org.apache.arrow.memory.util.LargeMemoryUtil.checkedCastToInt;
import static org.apache.arrow.util.Preconditions.checkNotNull;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.Collections;
import java.util.List;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;

/**
 * Importer for {@link ArrowArray}.
 */
final class ArrayImporter {
  private static final int MAX_IMPORT_RECURSION_LEVEL = 64;

  private final BufferAllocator allocator;
  private final FieldVector vector;
  private final DictionaryProvider dictionaryProvider;

  private ReferenceCountedArrowArray underlyingAllocation;
  private int recursionLevel;

  ArrayImporter(BufferAllocator allocator, FieldVector vector, DictionaryProvider dictionaryProvider) {
    this.allocator = Preconditions.checkNotNull(allocator);
    this.vector = Preconditions.checkNotNull(vector);
    this.dictionaryProvider = dictionaryProvider;
  }

  void importArray(ArrowArray src) {
    ArrowArray.Snapshot snapshot = src.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowArray");

    // Move imported array
    ArrowArray ownedArray = ArrowArray.allocateNew(allocator);
    ownedArray.save(snapshot);
    src.markReleased();
    src.close();

    recursionLevel = 0;

    // This keeps the array alive as long as there are any buffers that need it
    underlyingAllocation = new ReferenceCountedArrowArray(ownedArray);
    try {
      doImport(snapshot);
    } finally {
      underlyingAllocation.release();
    }
  }

  private void importChild(ArrayImporter parent, ArrowArray src) {
    ArrowArray.Snapshot snapshot = src.snapshot();
    checkState(snapshot.release != NULL, "Cannot import released ArrowArray");
    recursionLevel = parent.recursionLevel + 1;
    checkState(recursionLevel <= MAX_IMPORT_RECURSION_LEVEL, "Recursion level in ArrowArray struct exceeded");
    // Child buffers will keep the entire parent import alive.
    underlyingAllocation = parent.underlyingAllocation;
    doImport(snapshot);
  }

  private void doImport(ArrowArray.Snapshot snapshot) {
    // First import children (required for reconstituting parent array data)
    long[] children = NativeUtil.toJavaArray(snapshot.children, checkedCastToInt(snapshot.n_children));
    if (children != null && children.length > 0) {
      List<FieldVector> childVectors = vector.getChildrenFromFields();
      checkState(children.length == childVectors.size(), "ArrowArray struct has %s children (expected %s)",
          children.length, childVectors.size());
      for (int i = 0; i < children.length; i++) {
        checkState(children[i] != NULL, "ArrowArray struct has NULL child at position %s", i);
        ArrayImporter childImporter = new ArrayImporter(allocator, childVectors.get(i), dictionaryProvider);
        childImporter.importChild(this, ArrowArray.wrap(children[i]));
      }
    }

    // Handle import of a dictionary encoded vector
    if (snapshot.dictionary != NULL) {
      DictionaryEncoding encoding = vector.getField().getDictionary();
      checkNotNull(encoding, "Missing encoding on import of ArrowArray with dictionary");

      Dictionary dictionary = dictionaryProvider.lookup(encoding.getId());
      checkNotNull(dictionary, "Dictionary lookup failed on import of ArrowArray with dictionary");

      // reset the dictionary vector to the initial state
      dictionary.getVector().clear();

      ArrayImporter dictionaryImporter = new ArrayImporter(allocator, dictionary.getVector(), dictionaryProvider);
      dictionaryImporter.importChild(this, ArrowArray.wrap(snapshot.dictionary));
    }

    // Import main data
    ArrowFieldNode fieldNode = new ArrowFieldNode(snapshot.length, snapshot.null_count);
    long[] bufferPointers = NativeUtil.toJavaArray(snapshot.buffers, checkedCastToInt(snapshot.n_buffers));

    try (final BufferImportTypeVisitor visitor = new BufferImportTypeVisitor(
        allocator, underlyingAllocation, fieldNode, bufferPointers)) {
      final List<ArrowBuf> buffers;
      if (bufferPointers == null || bufferPointers.length == 0) {
        buffers = Collections.emptyList();
      } else {
        buffers = vector.getField().getType().accept(visitor);
      }
      vector.loadFieldBuffers(fieldNode, buffers);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Could not load buffers for field " + vector.getField() + ". error message: " + e.getMessage(), e);
    }
  }
}

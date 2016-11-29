<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Interprocess messaging / communication (IPC)

## File format

We define a self-contained "file format" containing an Arrow schema along with
one or more record batches defining a dataset. See [format/File.fbs][1] for the
precise details of the file metadata.

In general, the file looks like:

```
<magic number "ARROW1">
<empty padding bytes [to 64 byte boundary]>
<DICTIONARY 0>
...
<DICTIONARY k - 1>
<RECORD BATCH 0>
...
<RECORD BATCH n - 1>
<METADATA org.apache.arrow.flatbuf.Footer>
<metadata_size: int32>
<magic number "ARROW1">
```

See the File.fbs document for details about the Flatbuffers metadata. The
record batches have a particular structure, defined next.

### Record batches

The record batch metadata is written as a flatbuffer (see
[format/Message.fbs][2] -- the RecordBatch message type) prefixed by its size,
followed by each of the memory buffers in the batch written end to end (with
appropriate alignment and padding):

```
<int32: metadata flatbuffer size>
<metadata: org.apache.arrow.flatbuf.RecordBatch>
<padding bytes [to 64-byte boundary]>
<body: buffers end to end>
```

The `RecordBatch` metadata contains a depth-first (pre-order) flattened set of
field metadata and physical memory buffers (some comments from [Message.fbs][2]
have been shortened / removed):

```
table RecordBatch {
  length: int;
  nodes: [FieldNode];
  buffers: [Buffer];
}

struct FieldNode {
  /// The number of value slots in the Arrow array at this level of a nested
  /// tree
  length: int;

  /// The number of observed nulls. Fields with null_count == 0 may choose not
  /// to write their physical validity bitmap out as a materialized buffer,
  /// instead setting the length of the bitmap buffer to 0.
  null_count: int;
}

struct Buffer {
  /// The shared memory page id where this buffer is located. Currently this is
  /// not used
  page: int;

  /// The relative offset into the shared memory page where the bytes for this
  /// buffer starts
  offset: long;

  /// The absolute length (in bytes) of the memory buffer. The memory is found
  /// from offset (inclusive) to offset + length (non-inclusive).
  length: long;
}
```

In the context of a file, the `page` is not used, and the `Buffer` offsets use
as a frame of reference the start of the segment where they are written in the
file. So, while in a general IPC setting these offsets may be anyplace in one
or more shared memory regions, in the file format the offsets start from 0.

The location of a record batch and the size of the metadata block as well as
the body of buffers is stored in the file footer:

```
struct Block {
  offset: long;
  metaDataLength: int;
  bodyLength: long;
}
```

Some notes about this

* The `Block` offset indicates the starting byte of the record batch.
* The metadata length includes the flatbuffer size, the record batch metadata
  flatbuffer, and any padding bytes


### Dictionary batches

Dictionary batches have not yet been implemented, while they are provided for
in the metadata. For the time being, the `DICTIONARY` segments shown above in
the file do not appear in any of the file implementations.

[1]: https://github.com/apache/arrow/blob/master/format/File.fbs
[1]: https://github.com/apache/arrow/blob/master/format/Message.fbs
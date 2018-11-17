<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Interprocess messaging / communication (IPC)

## Encapsulated message format

Data components in the stream and file formats are represented as encapsulated
*messages* consisting of:

* A length prefix indicating the metadata size
* The message metadata as a [Flatbuffer][3]
* Padding bytes to an 8-byte boundary
* The message body, which must be a multiple of 8 bytes

Schematically, we have:

```
<metadata_size: int32>
<metadata_flatbuffer: bytes>
<padding>
<message body>
```

The complete serialized message must be a multiple of 8 bytes so that messages
can be relocated between streams. Otherwise the amount of padding between the
metadata and the message body could be non-deterministic.

The `metadata_size` includes the size of the flatbuffer plus padding. The
`Message` flatbuffer includes a version number, the particular message (as a
flatbuffer union), and the size of the message body:

```
table Message {
  version: org.apache.arrow.flatbuf.MetadataVersion;
  header: MessageHeader;
  bodyLength: long;
}
```

Currently, we support 4 types of messages:

* Schema
* RecordBatch
* DictionaryBatch
* Tensor

## Streaming format

We provide a streaming format for record batches. It is presented as a sequence
of encapsulated messages, each of which follows the format above. The schema
comes first in the stream, and it is the same for all of the record batches
that follow. If any fields in the schema are dictionary-encoded, one or more
`DictionaryBatch` messages will be included. `DictionaryBatch` and
`RecordBatch` messages may be interleaved, but before any dictionary key is used
in a `RecordBatch` it should be defined in a `DictionaryBatch`.

```
<SCHEMA>
<DICTIONARY 0>
...
<DICTIONARY k - 1>
<RECORD BATCH 0>
...
<DICTIONARY x DELTA>
...
<DICTIONARY y DELTA>
...
<RECORD BATCH n - 1>
<EOS [optional]: int32>
```

When a stream reader implementation is reading a stream, after each message, it
may read the next 4 bytes to know how large the message metadata that follows
is. Once the message flatbuffer is read, you can then read the message body.

The stream writer can signal end-of-stream (EOS) either by writing a 0 length
as an `int32` or simply closing the stream interface.

## File format

We define a "file format" supporting random access in a very similar format to
the streaming format. The file starts and ends with a magic string `ARROW1`
(plus padding). What follows in the file is identical to the stream format. At
the end of the file, we write a *footer* containing a redundant copy of the
schema (which is a part of the streaming format) plus memory offsets and sizes
for each of the data blocks in the file. This enables random access any record
batch in the file. See [format/File.fbs][1] for the precise details of the file
footer.

Schematically we have:

```
<magic number "ARROW1">
<empty padding bytes [to 8 byte boundary]>
<STREAMING FORMAT>
<FOOTER>
<FOOTER SIZE: int32>
<magic number "ARROW1">
```

In the file format, there is no requirement that dictionary keys should be
defined in a `DictionaryBatch` before they are used in a `RecordBatch`, as long
as the keys are defined somewhere in the file.

### RecordBatch body structure

The `RecordBatch` metadata contains a depth-first (pre-order) flattened set of
field metadata and physical memory buffers (some comments from [Message.fbs][2]
have been shortened / removed):

```
table RecordBatch {
  length: long;
  nodes: [FieldNode];
  buffers: [Buffer];
}

struct FieldNode {
  length: long;
  null_count: long;
}

struct Buffer {
  /// The relative offset into the shared memory page where the bytes for this
  /// buffer starts
  offset: long;

  /// The absolute length (in bytes) of the memory buffer. The memory is found
  /// from offset (inclusive) to offset + length (non-inclusive).
  length: long;
}
```

In the context of a file, the `page` is not used, and the `Buffer` offsets use
as a frame of reference the start of the message body. So, while in a general
IPC setting these offsets may be anyplace in one or more shared memory regions,
in the file format the offsets start from 0.

The location of a record batch and the size of the metadata block as well as
the body of buffers is stored in the file footer:

```
struct Block {
  offset: long;
  metaDataLength: int;
  bodyLength: long;
}
```

The `metaDataLength` here includes the metadata length prefix, serialized
metadata, and any additional padding bytes, and by construction must be a
multiple of 8 bytes.

Some notes about this

* The `Block` offset indicates the starting byte of the record batch.
* The metadata length includes the flatbuffer size, the record batch metadata
  flatbuffer, and any padding bytes

### Dictionary Batches

Dictionaries are written in the stream and file formats as a sequence of record
batches, each having a single field. The complete semantic schema for a
sequence of record batches, therefore, consists of the schema along with all of
the dictionaries. The dictionary types are found in the schema, so it is
necessary to read the schema to first determine the dictionary types so that
the dictionaries can be properly interpreted.

```
table DictionaryBatch {
  id: long;
  data: RecordBatch;
  isDelta: boolean = false;
}
```

The dictionary `id` in the message metadata can be referenced one or more times
in the schema, so that dictionaries can even be used for multiple fields. See
the [Physical Layout][4] document for more about the semantics of
dictionary-encoded data.

The dictionary `isDelta` flag allows dictionary batches to be modified
mid-stream.  A dictionary batch with `isDelta` set indicates that its vector
should be concatenated with those of any previous batches with the same `id`. A
stream which encodes one column, the list of strings
`["A", "B", "C", "B", "D", "C", "E", "A"]`, with a delta dictionary batch could
take the form:

```
<SCHEMA>
<DICTIONARY 0>
(0) "A"
(1) "B"
(2) "C"

<RECORD BATCH 0>
0
1
2
1

<DICTIONARY 0 DELTA>
(3) "D"
(4) "E"

<RECORD BATCH 1>
3
2
4
0
EOS
```

### Tensor (Multi-dimensional Array) Message Format

The `Tensor` message types provides a way to write a multidimensional array of
fixed-size values (such as a NumPy ndarray) using Arrow's shared memory
tools. Arrow implementations in general are not required to implement this data
format, though we provide a reference implementation in C++.

When writing a standalone encapsulated tensor message, we use the format as
indicated above, but additionally align the starting offset of the metadata as
well as the starting offset of the tensor body (if writing to a shared memory
region) to be multiples of 64 bytes:

```
<PADDING>
<metadata size: int32>
<metadata>
<tensor body>
```

[1]: https://github.com/apache/arrow/blob/master/format/File.fbs
[2]: https://github.com/apache/arrow/blob/master/format/Message.fbs
[3]: https://github.com/google/flatbuffers
[4]: https://github.com/apache/arrow/blob/master/format/Layout.md

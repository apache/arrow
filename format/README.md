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

## Arrow specification documents

> **Work-in-progress specification documents**. These are discussion documents
> created by the Arrow developers during late 2015 and in no way represents a
> finalized specification.

Currently, the Arrow specification consists of these pieces:

- Metadata specification (see Metadata.md)
- Physical memory layout specification (see Layout.md)
- Metadata serialized representation (see Message.fbs)
- Mechanics of messaging between Arrow systems (IPC, RPC, etc.) (see IPC.md)

The metadata currently uses Google's [flatbuffers library][1] for serializing a
couple related pieces of information:

- Schemas for tables or record (row) batches. This contains the logical types,
  field names, and other metadata. Schemas do not contain any information about
  actual data.
- *Data headers* for record (row) batches. These must correspond to a known
   schema, and enable a system to send and receive Arrow row batches in a form
   that can be precisely disassembled or reconstructed.

[1]: http://github.com/google/flatbuffers

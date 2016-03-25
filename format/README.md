## Arrow specification documents

> **Work-in-progress specification documents**. These are discussion documents
> created by the Arrow developers during late 2015 and in no way represents a
> finalized specification.

Currently, the Arrow specification consists of these pieces:

- Physical memory layout specification (see Layout.md)
- Metadata serialized representation (see Message.fbs)

The metadata currently uses Google's [flatbuffers library][1] for serializing a
couple related pieces of information:

- Schemas for tables or record (row) batches. This contains the logical types,
  field names, and other metadata. Schemas do not contain any information about
  actual data.
- *Data headers* for record (row) batches. These must correspond to a known
   schema, and enable a system to send and receive Arrow row batches in a form
   that can be precisely disassembled or reconstructed.

[1]: http://github.com/google/flatbuffers
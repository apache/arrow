# Arrow

[![Build Status](https://travis-ci.com/JuliaData/Arrow.jl.svg?branch=master)](https://travis-ci.com/JuliaData/Arrow.jl.svg?branch=master)
[![codecov](https://codecov.io/gh/JuliaData/Arrow.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/JuliaData/Arrow.jl)

This is a pure Julia implementation of the [Apache Arrow](https://arrow.apache.org) data standard.  This package provides Julia `AbstractVector` objects for
referencing data that conforms to the Arrow standard.  This allows users to seamlessly interface Arrow formatted data with a great deal of existing Julia code.

Please see this [document](https://arrow.apache.org/docs/format/Columnar.html#physical-memory-layout) for a description of the Arrow memory layout.

## Format Support

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages

It currently doesn't include support for:
  * Tensors or sparse tensors
  * Flight RPC
  * C data interface

Third-party data formats:
  * csv and parquet support via the existing CSV.jl and Parquet.jl packages
  * Other Tables.jl-compatible packages automatically supported (DataFrames.jl, JSONTables.jl, JuliaDB.jl, SQLite.jl, MySQL.jl, JDBC.jl, ODBC.jl, XLSX.jl, etc.)
  * No current Julia packages support ORC or Avro data formats

## Basic usage:

### Installation

```julia
] add Arrow
```

### Reading

#### `Arrow.Table`

    Arrow.Table(io::IO; convert::Bool=true)
    Arrow.Table(file::String; convert::Bool=true)
    Arrow.Table(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)

Read an arrow formatted table, from:
 * `io`, bytes will be read all at once via `read(io)`
 * `file`, bytes will be read via `Mmap.mmap(file)`
 * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`

Returns a `Arrow.Table` object that allows column access via `table.col1`, `table[:col1]`, or `table[1]`.

The Apache Arrow standard is foremost a "columnar" format and saves a variety of metadata with each column (such as column name, type, length, etc.).
A data set which has tens of thousands of columns is probably not well suited for the arrow format and may cause dramatic file size increases when one saves to a `arrow` file.
If it is possible to reshape the data such that there are not as many columns, `Arrow.Table` should not have as many problems. 
A simple method Julia provides is to simply execute `transpose(data)` to switch the rows and columns of your data if that does not interfere with one's analysis.

NOTE: the columns in an `Arrow.Table` are views into the original arrow memory, and hence are not easily
modifiable (with e.g. `push!`, `append!`, etc.). To mutate arrow columns, call `copy(x)` to materialize
the arrow data as a normal Julia array.

`Arrow.Table` also satisfies the Tables.jl interface, and so can easily be materialized via any supporting
sink function: e.g. `DataFrame(Arrow.Table(file))`, `SQLite.load!(db, "table", Arrow.Table(file))`, etc.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be
lazily converted to more friendly Julia defaults; by default, `convert=true`.

##### Examples

```julia
using Arrow

# read arrow table from file format
tbl = Arrow.Table(file)

# read arrow table from IO
tbl = Arrow.Table(io)

# read arrow table directly from bytes, like from an HTTP request
resp = HTTP.get(url)
tbl = Arrow.Table(resp.body)
```

#### `Arrow.Stream`

    Arrow.Stream(io::IO; convert::Bool=true)
    Arrow.Stream(file::String; convert::Bool=true)
    Arrow.Stream(bytes::Vector{UInt8}, pos=1, len=nothing; convert::Bool=true)

Start reading an arrow formatted table, from:
 * `io`, bytes will be read all at once via `read(io)`
 * `file`, bytes will be read via `Mmap.mmap(file)`
 * `bytes`, a byte vector directly, optionally allowing specifying the starting byte position `pos` and `len`

Reads the initial schema message from the arrow stream/file, then returns an `Arrow.Stream` object
which will iterate over record batch messages, producing an `Arrow.Table` on each iteration.

By iterating `Arrow.Table`, `Arrow.Stream` satisfies the `Tables.partitions` interface, and as such can
be passed to Tables.jl-compatible sink functions.

This allows iterating over extremely large "arrow tables" in chunks represented as record batches.

Supports the `convert` keyword argument which controls whether certain arrow primitive types will be
lazily converted to more friendly Julia defaults; by default, `convert=true`.

### Writing

#### `Arrow.write`

    Arrow.write(io::IO, tbl)
    Arrow.write(file::String, tbl)

Write any Tables.jl-compatible `tbl` out as arrow formatted data.
Providing an `io::IO` argument will cause the data to be written to it
in the "streaming" format, unless `file=true` keyword argument is passed.
Providing a `file::String` argument will result in the "file" format being written.

Multiple record batches will be written based on the number of
`Tables.partitions(tbl)` that are provided; by default, this is just
one for a given table, but some table sources support automatic
partitioning. Note you can turn multiple table objects into partitions
by doing `Tables.partitioner([tbl1, tbl2, ...])`, but note that
each table must have the exact same `Tables.Schema`.

By default, `Arrow.write` will use multiple threads to write multiple
record batches simultaneously (e.g. if julia is started with `julia -t 8`).

Supported keyword arguments to `Arrow.write` include:
  * `compress`: possible values include `:lz4`, `:zstd`, or your own initialized `LZ4FrameCompressor` or `ZstdCompressor` objects; will cause all buffers in each record batch to use the respective compression encoding
  * `alignment::Int=8`: specify the number of bytes to align buffers to when written in messages; strongly recommended to only use alignment values of 8 or 64 for modern memory cache line optimization
  * `dictencode::Bool=false`: whether all columns should use dictionary encoding when being written
  * `dictencodenested::Bool=false`: whether nested data type columns should also dict encode nested arrays/buffers; many other implementations don't support this
  * `denseunions::Bool=true`: whether Julia `Vector{<:Union}` arrays should be written using the dense union layout; passing `false` will result in the sparse union layout
  * `largelists::Bool=false`: causes list column types to be written with Int64 offset arrays; mainly for testing purposes; by default, Int64 offsets will be used only if needed
  * `file::Bool=false`: if a an `io` argument is being written to, passing `file=true` will cause the arrow file format to be written instead of just IPC streaming

##### Examples

```julia
# write directly to any IO in streaming format
Arrow.write(io, tbl)

# write to a file in file format
Arrow.write("data.arrow", tbl)
```

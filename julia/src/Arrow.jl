"""
    Arrow.jl

A pure Julia implementation of the [apache arrow](https://arrow.apache.org/) memory format specification.

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

See docs for official Arrow.jl API with `Arrow.Table`, `Arrow.write`, and `Arrow.Stream`.
"""
module Arrow

using Mmap
import Dates
using DataAPI, Tables, SentinelArrays, PooledArrays, CodecLz4, CodecZstd

using Base: @propagate_inbounds
import Base: ==

const DEBUG_LEVEL = Ref(0)

function setdebug!(level::Int)
    DEBUG_LEVEL[] = level
    return
end

function withdebug(f, level)
    lvl = DEBUG_LEVEL[]
    try
        setdebug!(level)
        f()
    finally
        setdebug!(lvl)
    end
end

macro debug(level, msg)
    esc(quote
        if DEBUG_LEVEL[] >= $level
            println(string("DEBUG: ", $(QuoteNode(__source__.file)), ":", $(QuoteNode(__source__.line)), " ", $msg))
        end
    end)
end

const ALIGNMENT = 8
const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"
const CONTINUATION_INDICATOR_BYTES = 0xffffffff

# vendored flatbuffers code for now
include("FlatBuffers/FlatBuffers.jl")
using .FlatBuffers

include("metadata/Flatbuf.jl")
using .Flatbuf; const Meta = Flatbuf

include("arrowtypes.jl")
using .ArrowTypes
include("utils.jl")
include("arraytypes.jl")
include("eltypes.jl")
include("table.jl")
include("write.jl")

end  # module Arrow

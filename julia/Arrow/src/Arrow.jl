# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
    Arrow.jl

A pure Julia implementation of the [apache arrow](https://arrow.apache.org/) memory format specification.

This implementation supports the 1.0 version of the specification, including support for:
  * All primitive data types
  * All nested data types
  * Dictionary encodings, nested dictionary encodings, and messages
  * Extension types
  * Streaming, file, record batch, and replacement and isdelta dictionary messages
  * Buffer compression/decompression via the standard LZ4 frame and Zstd formats

It currently doesn't include support for:
  * Tensors or sparse tensors
  * Flight RPC
  * C data interface

Third-party data formats:
  * csv and parquet support via the existing [CSV.jl](https://github.com/JuliaData/CSV.jl) and [Parquet.jl](https://github.com/JuliaIO/Parquet.jl) packages
  * Other [Tables.jl](https://github.com/JuliaData/Tables.jl)-compatible packages automatically supported ([DataFrames.jl](https://github.com/JuliaData/DataFrames.jl), [JSONTables.jl](https://github.com/JuliaData/JSONTables.jl), [JuliaDB.jl](https://github.com/JuliaData/JuliaDB.jl), [SQLite.jl](https://github.com/JuliaDatabases/SQLite.jl), [MySQL.jl](https://github.com/JuliaDatabases/MySQL.jl), [JDBC.jl](https://github.com/JuliaDatabases/JDBC.jl), [ODBC.jl](https://github.com/JuliaDatabases/ODBC.jl), [XLSX.jl](https://github.com/felipenoris/XLSX.jl), etc.)
  * No current Julia packages support ORC or Avro data formats

See docs for official Arrow.jl API with the [User Manual](@ref) and reference docs for [`Arrow.Table`](@ref), [`Arrow.write`](@ref), and [`Arrow.Stream`](@ref).
"""
module Arrow

using Mmap
import Dates
using DataAPI, Tables, SentinelArrays, PooledArrays, CodecLz4, CodecZstd, TimeZones, BitIntegers

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
include("arraytypes/arraytypes.jl")
include("eltypes.jl")
include("table.jl")
include("write.jl")

const LZ4_FRAME_COMPRESSOR = Ref{LZ4FrameCompressor}()
const ZSTD_COMPRESSOR = Ref{ZstdCompressor}()

function __init__()
    zstd = ZstdCompressor(; level=3)
    CodecZstd.TranscodingStreams.initialize(zstd)
    ZSTD_COMPRESSOR[] = zstd
    lz4 = LZ4FrameCompressor(; compressionlevel=4)
    CodecLz4.TranscodingStreams.initialize(lz4)
    LZ4_FRAME_COMPRESSOR[] = lz4
    return
end

end  # module Arrow

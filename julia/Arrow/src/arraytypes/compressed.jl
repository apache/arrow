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

struct CompressedBuffer
    data::Vector{UInt8}
    uncompressedlength::Int64
end

"""
    Arrow.Compressed

Represents the compressed version of an [`ArrowVector`](@ref).
Holds a reference to the original column. May have `Compressed`
children for nested array types.
"""
struct Compressed{Z, A}
    data::A
    buffers::Vector{CompressedBuffer}
    len::Int64
    nullcount::Int64
    children::Vector{Compressed}
end

Base.length(c::Compressed) = c.len
Base.eltype(c::Compressed{Z, A}) where {Z, A} = eltype(A)
getmetadata(x::Compressed) = getmetadata(x.data)
compressiontype(c::Compressed{Z}) where {Z} = Z

function compress(Z::Meta.CompressionType, comp, x::Array)
    GC.@preserve x begin
        y = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(x)), sizeof(x))
        return CompressedBuffer(transcode(comp, y), length(y))
    end
end

compress(Z::Meta.CompressionType, comp, x) = compress(Z, comp, convert(Array, x))

compress(Z::Meta.CompressionType, comp, v::ValidityBitmap) =
    v.nc == 0 ? CompressedBuffer(UInt8[], 0) : compress(Z, comp, view(v.bytes, v.pos:(v.pos + cld(v.ℓ, 8) - 1)))

function makenodesbuffers!(col::Compressed, fieldnodes, fieldbuffers, bufferoffset, alignment)
    push!(fieldnodes, FieldNode(col.len, col.nullcount))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    for buffer in col.buffers
        blen = length(buffer.data) == 0 ? 0 : 8 + length(buffer.data)
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
        bufferoffset += padding(blen, alignment)
    end
    for child in col.children
        bufferoffset = makenodesbuffers!(child, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    return bufferoffset
end

function writearray(io, b::CompressedBuffer)
    if length(b.data) > 0
        n = Base.write(io, b.uncompressedlength)
        @debug 1 "writing compressed buffer: uncompressedlength = $(b.uncompressedlength), n = $(length(b.data))"
        @debug 2 b.data
        return n + Base.write(io, b.data)
    end
    return 0
end

function writebuffer(io, col::Compressed, alignment)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    for buffer in col.buffers
        n = writearray(io, buffer)
        writezeros(io, paddinglength(n, alignment))
    end
    for child in col.children
        writebuffer(io, child, alignment)
    end
    return
end

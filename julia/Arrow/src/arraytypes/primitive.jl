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
    Arrow.Primitive

An `ArrowVector` where each element is a "fixed size" scalar of some kind, like an integer, float, decimal, or time type.
"""
struct Primitive{T, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    data::A
    ℓ::Int64
    metadata::Union{Nothing, Dict{String, String}}
end

Primitive(::Type{T}, b::Vector{UInt8}, v::ValidityBitmap, data::A, l, meta) where {T, A} =
    Primitive{T, A}(b, v, data, l, meta)

Base.size(p::Primitive) = (p.ℓ,)

function Base.copy(p::Primitive{T, A}) where {T, A}
    if nullcount(p) == 0 && T === eltype(A)
        return copy(p.data)
    else
        return convert(Array, p)
    end
end

@propagate_inbounds function Base.getindex(p::Primitive{T}, i::Integer) where {T}
    @boundscheck checkbounds(p, i)
    if T >: Missing
        return @inbounds (p.validity[i] ? ArrowTypes.arrowconvert(T, p.data[i]) : missing)
    else
        return @inbounds ArrowTypes.arrowconvert(T, p.data[i])
    end
end

@propagate_inbounds function Base.setindex!(p::Primitive{T}, v, i::Integer) where {T}
    @boundscheck checkbounds(p, i)
    if T >: Missing
        if v === missing
            @inbounds p.validity[i] = false
        else
            @inbounds p.data[i] = convert(Base.nonmissingtype(T), v)
        end
    else
        @inbounds p.data[i] = convert(Base.nonmissingtype(T), v)
    end
    return v
end

arrowvector(::PrimitiveType, x::Primitive, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::PrimitiveType, x, i, nl, fi, de, ded, meta; kw...)
    validity = ValidityBitmap(x)
    return Primitive(eltype(x), UInt8[], validity, x, length(x), meta)
end

function compress(Z::Meta.CompressionType, comp, p::P) where {P <: Primitive}
    len = length(p)
    nc = nullcount(p)
    validity = compress(Z, comp, p.validity)
    data = compress(Z, comp, p.data)
    return Compressed{Z, P}(p, [validity, data], len, nc, Compressed[])
end

function makenodesbuffers!(col::Primitive{T}, fieldnodes, fieldbuffers, bufferoffset, alignment) where {T}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    # adjust buffer offset, make primitive array buffer
    bufferoffset += blen
    blen = len * sizeof(Base.nonmissingtype(T))
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    return bufferoffset + padding(blen, alignment)
end

function writebuffer(io, col::Primitive{T}, alignment) where {T}
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col, alignment)
    n = writearray(io, Base.nonmissingtype(T), col.data)
    @debug 1 "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))
    return
end

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
    Arrow.BoolVector

A bit-packed array type, similar to [`ValidityBitmap`](@ref), but which
holds boolean values, `true` or `false`.
"""
struct BoolVector{T} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    pos::Int
    validity::ValidityBitmap
    ℓ::Int64
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(p::BoolVector) = (p.ℓ,)

@propagate_inbounds function Base.getindex(p::BoolVector{T}, i::Integer) where {T}
    @boundscheck checkbounds(p, i)
    if T >: Missing
        @inbounds !p.validity[i] && return missing
    end
    a, b = fldmod1(i, 8)
    @inbounds byte = p.arrow[p.pos + a - 1]
    # check individual bit of byte
    return getbit(byte, b)
end

@propagate_inbounds function Base.setindex!(p::BoolVector, v, i::Integer)
    @boundscheck checkbounds(p, i)
    x = convert(Bool, v)
    a, b = fldmod1(i, 8)
    @inbounds byte = p.arrow[p.pos + a - 1]
    @inbounds p.arrow[p.pos + a - 1] = setbit(byte, x, b)
    return v
end

arrowvector(::BoolType, x::BoolVector, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::BoolType, x, i, nl, fi, de, ded, meta; kw...)
    validity = ValidityBitmap(x)
    len = length(x)
    blen = cld(len, 8)
    bytes = Vector{UInt8}(undef, blen)
    b = 0xff
    j = k = 1
    for y in x
        if y === false
            b = setbit(b, false, j)
        end
        j += 1
        if j == 9
            @inbounds bytes[k] = b
            b = 0xff
            j = 1
            k += 1
        end
    end
    if j > 1
        bytes[k] = b
    end
    return BoolVector{eltype(x)}(bytes, 1, validity, len, meta)
end

function compress(Z::Meta.CompressionType, comp, p::P) where {P <: BoolVector}
    len = length(p)
    nc = nullcount(p)
    validity = compress(Z, comp, p.validity)
    data = compress(Z, comp, view(p.arrow, p.pos:(p.pos + cld(p.ℓ, 8) - 1)))
    return Compressed{Z, P}(p, [validity, data], len, nc, Compressed[])
end

function makenodesbuffers!(col::BoolVector, fieldnodes, fieldbuffers, bufferoffset, alignment)
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
    blen = bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    return bufferoffset + blen
end

function writebuffer(io, col::BoolVector, alignment)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col, alignment)
    n = Base.write(io, view(col.arrow, col.pos:(col.pos + cld(col.ℓ, 8) - 1)))
    return n + writezeros(io, paddinglength(n, alignment))
end

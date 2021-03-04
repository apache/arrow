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
    Arrow.FixedSizeList

An `ArrowVector` where each element is a "fixed size" list of some kind, like a `NTuple{N, T}`.
"""
struct FixedSizeList{T, A <: AbstractVector} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    data::A
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(l::FixedSizeList) = (l.ℓ,)

@propagate_inbounds function Base.getindex(l::FixedSizeList{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    N = ArrowTypes.getsize(Base.nonmissingtype(T))
    off = (i - 1) * N
    if Base.nonmissingtype(T) !== T
        return l.validity[i] ? ArrowTypes.arrowconvert(T, ntuple(j->l.data[off + j], N)) : missing
    else
        return ArrowTypes.arrowconvert(T, ntuple(j->l.data[off + j], N))
    end
end

@propagate_inbounds function Base.setindex!(l::FixedSizeList{T}, v::T, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    if v === missing
        @inbounds l.validity[i] = false
    else
        N = ArrowTypes.getsize(Base.nonmissingtype(T))
        off = (i - 1) * N
        foreach(1:N) do j
            @inbounds l.data[off + j] = v[j]
        end
    end
    return v
end

# lazy equal-spaced flattener
struct ToFixedSizeList{T, N, A} <: AbstractVector{T}
    data::A # A is AbstractVector of AbstractVector or AbstractString
end

function ToFixedSizeList(input)
    NT = Base.nonmissingtype(eltype(input)) # typically NTuple{N, T}
    return ToFixedSizeList{ArrowTypes.gettype(NT), ArrowTypes.getsize(NT), typeof(input)}(input)
end

Base.IndexStyle(::Type{<:ToFixedSizeList}) = Base.IndexLinear()
Base.size(x::ToFixedSizeList{T, N}) where {T, N} = (N * length(x.data),)

Base.@propagate_inbounds function Base.getindex(A::ToFixedSizeList{T, N}, i::Integer) where {T, N}
    @boundscheck checkbounds(A, i)
    a, b = fldmod1(i, N)
    @inbounds x = A.data[a]
    return @inbounds x === missing ? ArrowTypes.default(T) : x[b]
end

# efficient iteration
@inline function Base.iterate(A::ToFixedSizeList{T, N}, (i, chunk, chunk_i, len)=(1, 1, 1, length(A))) where {T, N}
    i > len && return nothing
    @inbounds y = A.data[chunk]
    @inbounds x = y === missing ? ArrowTypes.default(T) : y[chunk_i]
    if chunk_i == N
        chunk += 1
        chunk_i = 1
    else
        chunk_i += 1
    end
    return x, (i + 1, chunk, chunk_i, len)
end

arrowvector(::FixedSizeListType, x::FixedSizeList, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::FixedSizeListType, x, i, nl, fi, de, ded, meta; kw...)
    len = length(x)
    validity = ValidityBitmap(x)
    flat = ToFixedSizeList(x)
    if eltype(flat) == UInt8
        data = flat
    else
        data = arrowvector(flat, i, nl + 1, fi, de, ded, nothing; kw...)
    end
    return FixedSizeList{eltype(x), typeof(data)}(UInt8[], validity, data, len, meta)
end

function compress(Z::Meta.CompressionType, comp, x::FixedSizeList{T, A}) where {T, A}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    buffers = [validity]
    children = Compressed[]
    if eltype(A) == UInt8
        push!(buffers, compress(Z, comp, x.data))
    else
        push!(children, compress(Z, comp, x.data))
    end
    return Compressed{Z, typeof(x)}(x, buffers, len, nc, children)
end

function makenodesbuffers!(col::FixedSizeList{T, A}, fieldnodes, fieldbuffers, bufferoffset, alignment) where {T, A}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += blen
    if eltype(A) === UInt8
        blen = ArrowTypes.getsize(Base.nonmissingtype(T)) * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
        bufferoffset += padding(blen, alignment)
    else
        bufferoffset = makenodesbuffers!(col.data, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    return bufferoffset
end

function writebuffer(io, col::FixedSizeList{T, A}, alignment) where {T, A}
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col, alignment)
    # write values array
    if eltype(A) === UInt8
        n = writearray(io, UInt8, col.data)
        @debug 1 "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n, alignment))"
        writezeros(io, paddinglength(n, alignment))
    else
        writebuffer(io, col.data, alignment)
    end
    return
end

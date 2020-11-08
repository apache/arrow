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

# Union arrays
# need a custom representation of Union types since arrow unions
# are ordered, and possibly indirected via separate typeIds array
# here, T is Meta.UnionMode.Dense or Meta.UnionMode.Sparse,
# typeIds is a NTuple{N, Int32}, and U is a Tuple{...} of the
# unioned types
struct UnionT{T, typeIds, U}
end

unionmode(::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U} = T
typeids(::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U} = typeIds
Base.eltype(::Type{UnionT{T, typeIds, U}}) where {T, typeIds, U} = U

ArrowTypes.ArrowType(::Type{<:UnionT}) = ArrowTypes.UnionType()

# iterate a Julia Union{...} type, producing an array of unioned types
function eachunion(U::Union, elems=nothing)
    if elems === nothing
        return eachunion(U.b, Type[U.a])
    else
        push!(elems, U.a)
        return eachunion(U.b, elems)
    end
end

function eachunion(T, elems)
    push!(elems, T)
    return elems
end

# produce typeIds, offsets, data tuple for DenseUnion
isatypeid(x::T, ::Type{types}) where {T, types} = isatypeid(x, fieldtype(types, 1), types, 1)
isatypeid(x::T, ::Type{S}, ::Type{types}, i) where {T, S, types} = x isa S ? i : isatypeid(x, fieldtype(types, i + 1), types, i + 1)

"""
    Arrow.DenseUnion

An `ArrowVector` where the type of each element is one of a fixed set of types, meaning its eltype is like a julia `Union{type1, type2, ...}`.
An `Arrow.DenseUnion`, in comparison to `Arrow.SparseUnion`, stores elements in a set of arrays, one array per possible type, and an "offsets"
array, where each offset element is the index into one of the typed arrays. This allows a sort of "compression", where no extra space is
used/allocated to store all the elements.
"""
struct DenseUnion{T, S} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    arrow2::Vector{UInt8} # if arrow blob is compressed, need a 2nd reference for uncompressed offsets bytes
    typeIds::Vector{UInt8}
    offsets::Vector{Int32}
    data::S # Tuple of ArrowVector
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(s::DenseUnion) = size(s.typeIds)
nullcount(x::DenseUnion) = 0 # DenseUnion has no validity bitmap; only children do

@propagate_inbounds function Base.getindex(s::DenseUnion{T}, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    @inbounds typeId = s.typeIds[i]
    @inbounds off = s.offsets[i]
    @inbounds x = s.data[typeId + 1][off + 1]
    return x
end

@propagate_inbounds function Base.setindex!(s::DenseUnion{UnionT{T, typeIds, U}}, v, i::Integer) where {T, typeIds, U}
    @boundscheck checkbounds(s, i)
    @inbounds typeId = s.typeIds[i]
    typeids = typeIds === nothing ? (0:(fieldcount(U) - 1)) : typeIds
    vtypeId = Int8(typeids[isatypeid(v, U)])
    if typeId == vtypeId
        @inbounds off = s.offsets[i]
        @inbounds s.data[typeId +1][off + 1] = v
    else
        throw(ArgumentError("type of item to set $(typeof(v)) must match existing item $(fieldtype(U, typeid))"))
    end
    return v
end

# convenience wrappers for signaling that an array shoudld be written
# as with dense/sparse union arrow buffers
struct DenseUnionVector{T, U} <: AbstractVector{UnionT{Meta.UnionMode.Dense, nothing, U}}
    itr::T
end

DenseUnionVector(x::T) where {T} = DenseUnionVector{T, Tuple{eachunion(eltype(x))...}}(x)
Base.IndexStyle(::Type{<:DenseUnionVector}) = Base.IndexLinear()
Base.size(x::DenseUnionVector) = (length(x.itr),)
Base.iterate(x::DenseUnionVector, st...) = iterate(x.itr, st...)
Base.getindex(x::DenseUnionVector, i::Int) = getindex(x.itr, i)

function todense(::Type{UnionT{T, typeIds, U}}, x) where {T, typeIds, U}
    typeids = typeIds === nothing ? (0:(fieldcount(U) - 1)) : typeIds
    len = length(x)
    types = Vector{UInt8}(undef, len)
    offsets = Vector{Int32}(undef, len)
    data = Tuple(Vector{i == 1 ? Union{Missing, fieldtype(U, i)} : fieldtype(U, i)}(undef, 0) for i = 1:fieldcount(U))
    for (i, y) in enumerate(x)
        typeid = y === missing ? 0x00 : UInt8(typeids[isatypeid(y, U)])
        @inbounds types[i] = typeid
        @inbounds offsets[i] = length(data[typeid + 1])
        push!(data[typeid + 1], y)
    end
    return types, offsets, data
end

struct SparseUnionVector{T, U} <: AbstractVector{UnionT{Meta.UnionMode.Sparse, nothing, U}}
    itr::T
end

SparseUnionVector(x::T) where {T} = SparseUnionVector{T, Tuple{eachunion(eltype(x))...}}(x)
Base.IndexStyle(::Type{<:SparseUnionVector}) = Base.IndexLinear()
Base.size(x::SparseUnionVector) = (length(x.itr),)
Base.iterate(x::SparseUnionVector, st...) = iterate(x.itr, st...)
Base.getindex(x::SparseUnionVector, i::Int) = getindex(x.itr, i)

# sparse union child array producer
# for sparse unions, we split the parent array into
# N children arrays, each having the same length as the parent
# but with one child array per unioned type; each child
# should include the elements from parent of its type
# and other elements can be missing/default
function sparsetypeids(::Type{UnionT{T, typeIds, U}}, x) where {T, typeIds, U}
    typeids = typeIds === nothing ? (0:(fieldcount(U) - 1)) : typeIds
    len = length(x)
    types = Vector{UInt8}(undef, len)
    for (i, y) in enumerate(x)
        typeid = y === missing ? 0x00 : UInt8(typeids[isatypeid(y, U)])
        @inbounds types[i] = typeid
    end
    return types
end

struct ToSparseUnion{T, A} <: AbstractVector{T}
    data::A
end

ToSparseUnion(::Type{T}, data::A) where {T, A} = ToSparseUnion{T, A}(data)

Base.IndexStyle(::Type{<:ToSparseUnion}) = Base.IndexLinear()
Base.size(x::ToSparseUnion) = (length(x.data),)

Base.@propagate_inbounds function Base.getindex(A::ToSparseUnion{T}, i::Integer) where {T}
    @boundscheck checkbounds(A, i)
    @inbounds x = A.data[i]
    return @inbounds x isa T ? x : ArrowTypes.default(T)
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: DenseUnion}
    len = length(x)
    nc = nullcount(x)
    typeIds = compress(Z, comp, x.typeIds)
    offsets = compress(Z, comp, x.offsets)
    buffers = [typeIds, offsets]
    children = Compressed[]
    for y in x.data
        push!(children, compress(Z, comp, y))
    end
    return Compressed{Z, A}(x, buffers, len, nc, children)
end

"""
    Arrow.SparseUnion

An `ArrowVector` where the type of each element is one of a fixed set of types, meaning its eltype is like a julia `Union{type1, type2, ...}`.
An `Arrow.SparseUnion`, in comparison to `Arrow.DenseUnion`, stores elements in a set of arrays, one array per possible type, and each typed
array has the same length as the full array. This ends up with "wasted" space, since only one slot among the typed arrays is valid per full
array element, but can allow for certain optimizations when each typed array has the same length.
"""
struct SparseUnion{T, S} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    typeIds::Vector{UInt8}
    data::S # Tuple of ArrowVector
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(s::SparseUnion) = size(s.typeIds)
nullcount(x::SparseUnion) = 0

@propagate_inbounds function Base.getindex(s::SparseUnion{T}, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    @inbounds typeId = s.typeIds[i]
    @inbounds x = s.data[typeId + 1][i]
    return x
end

@propagate_inbounds function Base.setindex!(s::SparseUnion{UnionT{T, typeIds, U}}, v, i::Integer) where {T, typeIds, U}
    @boundscheck checkbounds(s, i)
    typeids = typeIds === nothing ? (0:(fieldcount(U) - 1)) : typeIds
    vtypeId = Int8(typeids[isatypeid(v, U)])
    @inbounds s.typeIds[i] = vtypeId
    @inbounds s.data[vtypeId + 1][i] = v
    return v
end

arrowvector(U::Union, x, i, nl, fi, de, ded, meta; denseunions::Bool=true, kw...) =
    arrowvector(denseunions ? DenseUnionVector(x) : SparseUnionVector(x), i, nl, fi, de, ded, meta; denseunions=denseunions, kw...)

arrowvector(::UnionType, x::Union{DenseUnion, SparseUnion}, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::UnionType, x, i, nl, fi, de, ded, meta; kw...)
    UT = eltype(x)
    if unionmode(UT) == Meta.UnionMode.Dense
        x = x isa DenseUnionVector ? x.itr : x
        typeids, offsets, data = todense(UT, x)
        data2 = map(y -> arrowvector(y[2], i, nl + 1, y[1], de, ded, nothing; kw...), enumerate(data))
        return DenseUnion{UT, typeof(data2)}(UInt8[], UInt8[], typeids, offsets, data2, meta)
    else
        x = x isa SparseUnionVector ? x.itr : x
        typeids = sparsetypeids(UT, x)
        data3 = Tuple(arrowvector(ToSparseUnion(fieldtype(eltype(UT), j), x), i, nl + 1, j, de, ded, nothing; kw...) for j = 1:fieldcount(eltype(UT)))
        return SparseUnion{UT, typeof(data3)}(UInt8[], typeids, data3, meta)
    end
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: SparseUnion}
    len = length(x)
    nc = nullcount(x)
    typeIds = compress(Z, comp, x.typeIds)
    buffers = [typeIds]
    children = Compressed[]
    for y in x.data
        push!(children, compress(Z, comp, y))
    end
    return Compressed{Z, A}(x, buffers, len, nc, children)
end

function makenodesbuffers!(col::Union{DenseUnion, SparseUnion}, fieldnodes, fieldbuffers, bufferoffset, alignment)
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # typeIds buffer
    push!(fieldbuffers, Buffer(bufferoffset, len))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += padding(len, alignment)
    if col isa DenseUnion
        # offsets buffer
        blen = sizeof(Int32) * len
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
        bufferoffset += padding(blen, alignment)
    end
    for child in col.data
        bufferoffset = makenodesbuffers!(child, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    return bufferoffset
end

function writebuffer(io, col::Union{DenseUnion, SparseUnion}, alignment)
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    # typeIds buffer
    n = writearray(io, UInt8, col.typeIds)
    @debug 1 "writing array: col = $(typeof(col.typeIds)), n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))
    if col isa DenseUnion
        n = writearray(io, Int32, col.offsets)
        @debug 1 "writing array: col = $(typeof(col.offsets)), n = $n, padded = $(padding(n, alignment))"
        writezeros(io, paddinglength(n, alignment))
    end
    for child in col.data
        writebuffer(io, child, alignment)
    end
    return
end

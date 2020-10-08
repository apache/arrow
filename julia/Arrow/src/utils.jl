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
    padding(n::Integer)

Determines the total number of bytes needed to store `n` bytes with padding.
Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
"""
padding(n::Integer, alignment) = ((n + alignment - 1) ÷ alignment) * alignment

paddinglength(n::Integer, alignment) = padding(n, alignment) - n

function writezeros(io::IO, n::Integer)
    s = 0
    for i ∈ 1:n
        s += Base.write(io, 0x00)
    end
    s
end

# efficient writing of arrays
writearray(io, col) = writearray(io, maybemissing(eltype(col)), col)

function writearray(io::IO, ::Type{T}, col) where {T}
    if col isa Vector{T}
        n = Base.write(io, col)
    elseif isbitstype(T) && (col isa Vector{Union{T, Missing}} || col isa SentinelVector{T, T, Missing, Vector{T}})
        # need to write the non-selector bytes of isbits Union Arrays
        n = Base.unsafe_write(io, pointer(col), sizeof(T) * length(col))
    elseif col isa ChainedVector
        n = 0
        for A in col.arrays
            n += writearray(io, T, A)
        end
    else
        n = 0
        for x in col
            n += Base.write(io, coalesce(x, ArrowTypes.default(T)))
        end
    end
    return n
end

"""
    getbit

This deliberately elides bounds checking.
"""
getbit(v::UInt8, n::Integer) = Bool((v & 0x02^(n - 1)) >> (n - 1))

"""
    setbit

This also deliberately elides bounds checking.
"""
function setbit(v::UInt8, b::Bool, n::Integer)
    if b
        v | 0x02^(n - 1)
    else
        v & (0xff ⊻ 0x02^(n - 1))
    end
end

"""
    bitpackedbytes(n)

Determines the number of bytes used by `n` bits, optionally with padding.
"""
function bitpackedbytes(n::Integer, alignment)
    ℓ = cld(n, 8)
    return ℓ + paddinglength(ℓ, alignment)
end

# count # of missing elements in an iterable
nullcount(col) = count(ismissing, col)

# like startswith/endswith for strings, but on byte buffers
function _startswith(a::AbstractVector{UInt8}, pos::Integer, b::AbstractVector{UInt8})
    for i = 1:length(b)
        @inbounds check = a[pos + i - 1] == b[i]
        check || return false
    end
    return true
end

function _endswith(a::AbstractVector{UInt8}, endpos::Integer, b::AbstractVector{UInt8})
    aoff = endpos - length(b) + 1
    for i = 1:length(b)
        @inbounds check = a[aoff] == b[i]
        check || return false
        aoff += 1
    end
    return true
end

# read a single element from a byte vector
# copied from read(::IOBuffer, T) in Base
function readbuffer(t::AbstractVector{UInt8}, pos::Integer, ::Type{T}) where {T}
    GC.@preserve t begin
        ptr::Ptr{T} = pointer(t, pos)
        x = unsafe_load(ptr)
    end
end

# # argh me mateys, don't nobody else go pirating this method
# # this here be me own booty!
# if !applicable(iterate, missing)
# Base.iterate(::Missing, st=1) = st === nothing ? nothing : (missing, nothing)
# end

getK(::Type{Pair{K, V}}) where {K, V} = K
getV(::Type{Pair{K, V}}) where {K, V} = V
getN(::Type{NTuple{N, T}}) where {N, T} = N
getT(::Type{NTuple{N, T}}) where {N, T} = T
getnames(::Type{NamedTuple{names, T}}) where {names, T} = names
getT(::Type{NamedTuple{names, T}}) where {names, T} = T
getN(::Type{NamedTuple{names, T}}) where {names, T} = length(names)

encodingtype(n) = n < div(typemax(Int8), 2) ? Int8 : n < div(typemax(Int16), 2) ? Int16 : n < div(typemax(Int32), 2) ? Int32 : Int64

struct Converter{T, A} <: AbstractVector{T}
    data::A
end

converter(::Type{T}, x::A) where {T, A} = Converter{eltype(A) >: Missing ? Union{T, Missing} : T, A}(x)
converter(::Type{T}, x::ChainedVector{A}) where {T, A} = ChainedVector([converter(T, x) for x in x.arrays])

Base.IndexStyle(::Type{<:Converter}) = Base.IndexLinear()
Base.size(x::Converter) = (length(x.data),)
Base.eltype(x::Converter{T, A}) where {T, A} = T
Base.getindex(x::Converter{T}, i::Int) where {T} = ArrowTypes.arrowconvert(T, getindex(x.data, i))

maybemissing(::Type{T}) where {T} = T === Missing ? Missing : Base.nonmissingtype(T)

macro miss_or(x, ex)
    esc(:($x === missing ? missing : $(ex)))
end

function getfooter(filebytes)
    len = readbuffer(filebytes, length(filebytes) - 9, Int32)
    FlatBuffers.getrootas(Meta.Footer, filebytes[end-(9 + len):end-10], 0)
end

function getrb(filebytes)
    f = getfooter(filebytes)
    rb = f.recordBatches[1]
    return filebytes[rb.offset+1:(rb.offset+1+rb.metaDataLength)]
    # FlatBuffers.getrootas(Meta.Message, filebytes, rb.offset)
end

function readmessage(filebytes, off=9)
    @assert readbuffer(filebytes, off, UInt32) === 0xFFFFFFFF
    len = readbuffer(filebytes, off + 4, Int32)

    FlatBuffers.getrootas(Meta.Message, filebytes, off + 8)
end

# an AbstractVector version of Iterators.flatten
# code based on SentinelArrays.ChainedVector
struct ToList{T, A, I} <: AbstractVector{T}
    data::Vector{A} # A is AbstractVector or AbstractString
    inds::Vector{I}
end

stringtype(T) = false
stringtype(::Type{T}) where {T <: AbstractString} = true

function ToList(input; largelists::Bool=false)
    AT = eltype(input)
    ST = Base.nonmissingtype(AT)
    T = stringtype(ST) ? UInt8 : eltype(ST)
    len = stringtype(ST) ? sizeof : length
    data = AT[]
    I = largelists ? Int64 : Int32
    inds = I[0]
    sizehint!(data, length(input))
    sizehint!(inds, length(input))
    totalsize = I(0)
    for x in input
        if x === missing
            push!(data, missing)
        else
            push!(data, x)
            totalsize += len(x)
            if I === Int32 && totalsize > 2147483647
                I = Int64
                inds = convert(Vector{Int64}, inds)
            end
        end
        push!(inds, totalsize)
    end
    return ToList{T, AT, I}(data, inds)
end

Base.IndexStyle(::Type{<:ToList}) = Base.IndexLinear()
Base.size(x::ToList) = (length(x.inds) == 0 ? 0 : x.inds[end],)

function Base.pointer(A::ToList{UInt8}, i::Integer)
    chunk = searchsortedfirst(A.inds, i)
    return pointer(A.data[chunk - 1])
end

@inline function index(A::ToList, i::Integer)
    chunk = searchsortedfirst(A.inds, i)
    return chunk - 1, i - (@inbounds A.inds[chunk - 1])
end

Base.@propagate_inbounds getchunk(data, chunk) = @inbounds data[chunk]
Base.@propagate_inbounds getchunk(data::Vector{A}, chunk) where {A <: Union{Missing, AbstractString}} = @inbounds codeunits(data[chunk])

Base.@propagate_inbounds function Base.getindex(A::ToList, i::Integer)
    @boundscheck checkbounds(A, i)
    chunk, ix = index(A, i)
    @inbounds x = getchunk(A.data, chunk)[ix]
    return x
end

Base.@propagate_inbounds function Base.setindex!(A::ToList, v, i::Integer)
    @boundscheck checkbounds(A, i)
    chunk, ix = index(A, i)
    @inbounds getchunk(A.data, chunk)[ix] = v
    return v
end

# efficient iteration
@inline function Base.iterate(A::ToList)
    length(A) == 0 && return nothing
    i = 1
    chunk = 2
    chunk_i = 1
    chunk_len = A.inds[chunk]
    while i > chunk_len
        chunk += 1
        chunk_len = A.inds[chunk]
    end
    x = getchunk(A.data, chunk - 1)[1]
    # find next valid index
    i += 1
    if i > chunk_len
        while true
            chunk += 1
            chunk > length(A.inds) && break
            chunk_len = A.inds[chunk]
            i <= chunk_len && break
        end
    else
        chunk_i += 1
    end
    return x, (i, chunk, chunk_i, chunk_len, length(A))
end

@inline function Base.iterate(A::ToList, (i, chunk, chunk_i, chunk_len, len))
    i > len && return nothing
    @inbounds x = getchunk(A.data, chunk - 1)[chunk_i]
    i += 1
    if i > chunk_len
        chunk_i = 1
        while true
            chunk += 1
            chunk > length(A.inds) && break
            @inbounds chunk_len = A.inds[chunk]
            i <= chunk_len && break
        end
    else
        chunk_i += 1
    end
    return x, (i, chunk, chunk_i, chunk_len, len)
end

# 
struct ToFixedSizeList{T, N, A} <: AbstractVector{T}
    data::A # A is AbstractVector or AbstractString
end

function ToFixedSizeList(input)
    NT = Base.nonmissingtype(eltype(input)) # NTuple{N, T}
    return ToFixedSizeList{getT(NT), getN(NT), typeof(input)}(input)
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

struct ToStruct{T, i, A} <: AbstractVector{T}
    data::A # eltype is NamedTuple
end

ToStruct(x::A, j::Integer) where {A} = ToStruct{fieldtype(Base.nonmissingtype(eltype(A)), j), j, A}(x)

Base.IndexStyle(::Type{<:ToStruct}) = Base.IndexLinear()
Base.size(x::ToStruct) = (length(x.data),)

Base.@propagate_inbounds function Base.getindex(A::ToStruct{T, j}, i::Integer) where {T, j}
    @boundscheck checkbounds(A, i)
    @inbounds x = A.data[i]
    return @miss_or(x, @inbounds getfield(x, j))
end

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

# convenience wrappers for signaling that an array shoudld be written
# as with dense/sparse union arrow buffers
struct DenseUnionVector{T, U} <: AbstractVector{UnionT{Meta.UnionMode.Dense, nothing, U}}
    itr::T
end

DenseUnionVector(x::T) where {T} = DenseUnionVector{T, Tuple{eachunion(eltype(x))...}}(x)
Base.IndexStyle(::Type{<:DenseUnionVector}) = Base.IndexLinear()
Base.size(x::DenseUnionVector) = (length(x.itr),)
Base.eltype(x::DenseUnionVector{T, U}) where {T, U} = UnionT{Meta.UnionMode.Dense, nothing, U}
Base.iterate(x::DenseUnionVector, st...) = iterate(x.itr, st...)
Base.getindex(x::DenseUnionVector, i::Int) = getindex(x.itr, i)

struct SparseUnionVector{T, U} <: AbstractVector{UnionT{Meta.UnionMode.Sparse, nothing, U}}
    itr::T
end

SparseUnionVector(x::T) where {T} = SparseUnionVector{T, Tuple{eachunion(eltype(x))...}}(x)
Base.IndexStyle(::Type{<:SparseUnionVector}) = Base.IndexLinear()
Base.size(x::SparseUnionVector) = (length(x.itr),)
Base.eltype(x::SparseUnionVector{T, U}) where {T, U} = UnionT{Meta.UnionMode.Sparse, nothing, U}
Base.iterate(x::SparseUnionVector, st...) = iterate(x.itr, st...)
Base.getindex(x::SparseUnionVector, i::Int) = getindex(x.itr, i)

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

# ToMap
struct ToMap{T, A, I} <: AbstractVector{T}
    data::Vector{A} # A is a Vector{KeyValue{K, V}}
    inds::Vector{I}
end

function ToMap(input; largelists::Bool=false)
    AT = eltype(input)
    ST = Base.nonmissingtype(AT)
    PT = eltype(ST)
    K, V = getK(PT), getV(PT)
    T = KeyValue{K, V}
    VT = AT !== ST ? Union{Missing, Vector{T}} : Vector{T}
    data = VT[]
    I = largelists ? Int64 : Int32
    inds = I[0]
    sizehint!(data, length(input))
    sizehint!(inds, length(input))
    totalsize = I(0)
    for x in input
        if x === missing
            push!(data, missing)
        else
            push!(data, [KeyValue{K, V}(k, v) for (k, v) in pairs(x)])
            totalsize += length(x)
            if I === Int32 && totalsize > 2147483647
                I = Int64
                inds = convert(Vector{Int64}, inds)
            end
        end
        push!(inds, totalsize)
    end
    return ToMap{T, VT, I}(data, inds)
end

Base.IndexStyle(::Type{<:ToMap}) = Base.IndexLinear()
Base.size(x::ToMap) = (length(x.inds) == 0 ? 0 : x.inds[end],)

@inline function index(A::ToMap, i::Integer)
    chunk = searchsortedfirst(A.inds, i)
    return chunk - 1, i - (@inbounds A.inds[chunk - 1])
end

Base.@propagate_inbounds function Base.getindex(A::ToMap, i::Integer)
    @boundscheck checkbounds(A, i)
    chunk, ix = index(A, i)
    @inbounds x = A.data[chunk][ix]
    return x
end

Base.@propagate_inbounds function Base.setindex!(A::ToMap, v, i::Integer)
    @boundscheck checkbounds(A, i)
    chunk, ix = index(A, i)
    @inbounds A.data[chunk][ix] = v
    return v
end

# efficient iteration
@inline function Base.iterate(A::ToMap)
    length(A) == 0 && return nothing
    i = 1
    chunk = 1
    chunk_i = 1
    chunk_len = A.inds[chunk + 1]
    while i > chunk_len
        chunk += 1
        @inbounds chunk_len = A.inds[chunk + 1]
    end
    x = A.data[chunk][1]
    # find next valid index
    i += 1
    if i > chunk_len
        while true
            chunk += 1
            chunk > length(A.inds) && break
            @inbounds chunk_len = A.inds[chunk + 1]
            i <= chunk_len && break
        end
    else
        chunk_i += 1
    end
    return x, (i, chunk, chunk_i, chunk_len, length(A))
end

@inline function Base.iterate(A::ToMap, (i, chunk, chunk_i, chunk_len, len))
    i > len && return nothing
    @inbounds x = A.data[chunk][chunk_i]
    i += 1
    if i > chunk_len
        chunk_i = 1
        while true
            chunk += 1
            chunk > length(A.inds) && break
            @inbounds chunk_len = A.inds[chunk + 1]
            i <= chunk_len && break
        end
    else
        chunk_i += 1
    end
    return x, (i, chunk, chunk_i, chunk_len, len)
end
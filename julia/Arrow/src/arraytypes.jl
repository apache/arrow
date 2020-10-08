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

abstract type ArrowVector{T} <: AbstractVector{T} end

Base.IndexStyle(::Type{A}) where {A <: ArrowVector} = Base.IndexLinear()
Base.similar(::Type{A}, dims::Dims) where {T, A <: ArrowVector{T}} = Vector{T}(undef, dims)
validitybitmap(x::ArrowVector) = x.validity
nullcount(x::ArrowVector) = validitybitmap(x).nc
getmetadata(x::ArrowVector) = x.metadata

function toarrowvector(x, de=DictEncoding[], meta=getmetadata(x); compression::Union{Nothing, LZ4FrameCompressor, ZstdCompressor}=nothing, kw...)
    @debug 2 "converting top-level column to arrow format: col = $(typeof(x)), compression = $compression, kw = $(kw.data)"
    @debug 3 x
    A = arrowvector(x, de, meta; compression=compression, kw...)
    if compression isa LZ4FrameCompressor
        A = compress(Meta.CompressionType.LZ4_FRAME, compression, A)
    elseif compression isa ZstdCompressor
        A = compress(Meta.CompressionType.ZSTD, compression, A)
    end
    @debug 2 "converted top-level column to arrow format: $(typeof(A))"
    @debug 3 A
    return A
end

function arrowvector(x, de, meta; dictencoding::Bool=false, dictencode::Bool=false, kw...)
    if !(x isa DictEncode) && !dictencoding && (dictencode || (x isa AbstractArray && DataAPI.refarray(x) !== x))
        x = DictEncode(x)
    end
    T = eltype(x)
    S = maybemissing(T)
    return arrowvector(S, T, x, de, meta; kw...)
end

# conversions to arrow types
arrowvector(::Type{Dates.Date}, ::Type{S}, x, de, meta; kw...) where {S} =
    arrowvector(converter(DATE, x), de, meta; kw...)
arrowvector(::Type{Dates.Time}, ::Type{S}, x, de, meta; kw...) where {S} =
    arrowvector(converter(TIME, x), de, meta; kw...)
arrowvector(::Type{Dates.DateTime}, ::Type{S}, x, de, meta; kw...) where {S} =
    arrowvector(converter(DATETIME, x), de, meta; kw...)
arrowvector(::Type{P}, ::Type{S}, x, de, meta; kw...) where {P <: Dates.Period, S} =
    arrowvector(converter(Duration{arrowperiodtype(P)}, x), de, meta; kw...)

# fallback that calls ArrowType
function arrowvector(::Type{S}, ::Type{T}, x, de, meta; kw...) where {S, T}
    if ArrowTypes.istyperegistered(S)
        meta = meta === nothing ? Dict{String, String}() : meta
        arrowtype = ArrowTypes.getarrowtype!(meta, S)
        return arrowvector(converter(arrowtype, x), de, meta; kw...)
    end
    return arrowvector(ArrowType(S), S, T, x, de, meta; kw...)
end

arrowvector(::NullType, ::Type{Missing}, ::Type{Missing}, x, de, meta; kw...) = MissingVector(length(x))
compress(Z::Meta.CompressionType, comp, v::MissingVector) =
    Compressed{Z, MissingVector}(v, CompressedBuffer[], length(v), length(v), Compressed[])

struct ValidityBitmap <: ArrowVector{Bool}
    bytes::Vector{UInt8} # arrow memory blob
    pos::Int # starting byte of validity bitmap
    ℓ::Int # # of _elements_ (not bytes!) in bitmap (because bitpacking)
    nc::Int # null count
end

compress(Z::Meta.CompressionType, comp, v::ValidityBitmap) =
    v.nc == 0 ? CompressedBuffer(UInt8[], 0) : compress(Z, comp, view(v.bytes, v.pos:(v.pos + cld(v.ℓ, 8) - 1)))

Base.size(p::ValidityBitmap) = (p.ℓ,)
nullcount(x::ValidityBitmap) = x.nc

function ValidityBitmap(len::Integer)
    return ValidityBitmap(UInt8[], 1, len, 0)
end

function ValidityBitmap(x)
    len = length(x)
    blen = cld(len, 8)
    bytes = Vector{UInt8}(undef, blen)
    st = iterate(x)
    i = 0
    nc = 0
    for k = 1:blen
        b = 0x00
        for j = 1:8
            if (i + j) <= len
                y, state = st
                if !y
                    nc += 1
                    b = setbit(b, false, j)
                else
                    b = setbit(b, true, j)
                end
                st = iterate(x, state)
            end
        end
        i += 8
        @inbounds bytes[k] = b
    end
    return ValidityBitmap(nc == 0 ? UInt8[] : bytes, 1, nc == 0 ? 0 : len, nc)
end

@propagate_inbounds function Base.getindex(p::ValidityBitmap, i::Integer)
    # no boundscheck because parent array should do it
    # if a validity bitmap is empty, it either means:
    #   1) the parent array null_count is 0, so all elements are valid
    #   2) parent array is also empty, so "all" elements are valid
    p.nc == 0 && return true
    # translate element index to bitpacked byte index
    a, b = fldmod1(i, 8)
    @inbounds byte = p.bytes[p.pos + a - 1]
    # check individual bit of byte
    return getbit(byte, b)
end

@propagate_inbounds function Base.setindex!(p::ValidityBitmap, v, i::Integer)
    x = convert(Bool, v)
    p.ℓ == 0 && !x && throw(BoundsError(p, i))
    a, b = fldmod1(i, 8)
    @inbounds byte = p.bytes[p.pos + a - 1]
    @inbounds p.bytes[p.pos + a - 1] = setbit(byte, x, b)
    return v
end

struct Primitive{T, S, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    data::A
    ℓ::Int64
    metadata::Union{Nothing, Dict{String, String}}
end

Primitive(::Type{T}, b::Vector{UInt8}, v::ValidityBitmap, data::A, l::Int, meta) where {T, A} =
    Primitive{T, eltype(A), A}(b, v, data, l, meta)

Base.size(p::Primitive) = (p.ℓ,)

function arrowvector(::PrimitiveType, ::Type{T}, ::Type{S}, x, de, meta; kw...) where {T, S}
    len = length(x)
    if T !== S
        validity = ValidityBitmap((y !== missing for y in x))
    else
        validity = ValidityBitmap(len)
    end
    return Primitive{S, T, typeof(x)}(UInt8[], validity, x, len, meta)
end

function Base.copy(p::Primitive{T, S}) where {T, S}
    if T === S
        return copy(p.data)
    else
        return convert(Array, p)
    end
end

@propagate_inbounds function Base.getindex(p::Primitive{T, S}, i::Integer) where {T, S}
    @boundscheck checkbounds(p, i)
    if T >: Missing
        return @inbounds (p.validity[i] ? ArrowTypes.arrowconvert(T, p.data[i]) : missing)
    else
        return @inbounds ArrowTypes.arrowconvert(T, p.data[i])
    end
end

@propagate_inbounds function Base.setindex!(p::Primitive{T, S}, v, i::Integer) where {T, S}
    @boundscheck checkbounds(p, i)
    if T >: Missing
        if v === missing
            @inbounds p.validity[i] = false
        else
            @inbounds p.data[i] = convert(S, v)
        end
    else
        @inbounds p.data[i] = convert(S, v)
    end
    return v
end

function compress(Z::Meta.CompressionType, comp, p::P) where {P <: Primitive}
    len = length(p)
    nc = nullcount(p)
    validity = compress(Z, comp, p.validity)
    data = compress(Z, comp, p.data)
    return Compressed{Z, P}(p, [validity, data], len, nc, Compressed[])
end

struct Offsets{T <: Union{Int32, Int64}} <: ArrowVector{Tuple{T, T}}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    offsets::Vector{T}
end

Base.size(o::Offsets) = (length(o.offsets) - 1,)

@propagate_inbounds function Base.getindex(o::Offsets, i::Integer)
    @boundscheck checkbounds(o, i)
    @inbounds lo = o.offsets[i] + 1
    @inbounds hi = o.offsets[i + 1]
    return lo, hi
end

struct List{T, O, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    offsets::Offsets{O}
    data::A
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(l::List) = (l.ℓ,)

function arrowvector(::ListType, ::Type{T}, ::Type{S}, x, de, meta; largelists::Bool=false, kw...) where {T, S}
    len = length(x)
    if S !== T
        validity = ValidityBitmap((y !== missing for y in x))
    else
        validity = ValidityBitmap(len)
    end
    flat = ToList(x; largelists=largelists)
    offsets = Offsets(UInt8[], flat.inds)
    if eltype(flat) == UInt8
        data = flat
    else
        data = arrowvector(flat, de, nothing; lareglists=largelists, kw...)
    end
    return List{S, eltype(flat.inds), typeof(data)}(UInt8[], validity, offsets, data, len, meta)
end

@propagate_inbounds function Base.getindex(l::List{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds lo, hi = l.offsets[i]
    if ArrowTypes.isstringtype(T)
        if Base.nonmissingtype(T) !== T
            return l.validity[i] ? ArrowTypes.arrowconvert(T, unsafe_string(pointer(l.data, lo), hi - lo + 1)) : missing
        else
            return ArrowTypes.arrowconvert(T, unsafe_string(pointer(l.data, lo), hi - lo + 1))
        end
    elseif Base.nonmissingtype(T) !== T
        return l.validity[i] ? ArrowTypes.arrowconvert(T, view(l.data, lo:hi)) : missing
    else
        return ArrowTypes.arrowconvert(T, view(l.data, lo:hi))
    end
end

# @propagate_inbounds function Base.setindex!(l::List{T}, v, i::Integer) where {T}

# end

function compress(Z::Meta.CompressionType, comp, x::List{T, O, A}) where {T, O, A}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    offsets = compress(Z, comp, x.offsets.offsets)
    buffers = [validity, offsets]
    children = Compressed[]
    if eltype(A) == UInt8
        push!(buffers, compress(Z, comp, x.data))
    else
        push!(children, compress(Z, comp, x.data))
    end
    return Compressed{Z, typeof(x)}(x, buffers, len, nc, children)
end

struct FixedSizeList{T, A <: AbstractVector} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    data::A
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(l::FixedSizeList) = (l.ℓ,)

function arrowvector(::FixedSizeListType, ::Type{T}, ::Type{S}, x, de, meta; kw...) where {T, S}
    len = length(x)
    if S !== T
        validity = ValidityBitmap((y !== missing for y in x))
    else
        validity = ValidityBitmap(len)
    end
    flat = ToFixedSizeList(x)
    if eltype(flat) == UInt8
        data = flat
    else
        data = arrowvector(flat, de, nothing; kw...)
    end
    return FixedSizeList{S, typeof(data)}(UInt8[], validity, data, len, meta)
end

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

struct Map{T, O, A} <: ArrowVector{T}
    validity::ValidityBitmap
    offsets::Offsets{O}
    data::A
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(l::Map) = (l.ℓ,)

function arrowvector(::MapType, ::Type{T}, ::Type{S}, x, de, meta; largelists::Bool=false, kw...) where {T, S}
    len = length(x)
    if S !== T
        validity = ValidityBitmap((y !== missing for y in x))
    else
        validity = ValidityBitmap(len)
    end
    flat = ToMap(x; largelists=largelists)
    offsets = Offsets(UInt8[], flat.inds)
    data = arrowvector(flat, de, nothing; lareglists=largelists, kw...)
    return Map{S, eltype(flat.inds), typeof(data)}(validity, offsets, data, len, meta)
end

@propagate_inbounds function Base.getindex(l::Map{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds lo, hi = l.offsets[i]
    if Base.nonmissingtype(T) !== T
        return l.validity[i] ? ArrowTypes.arrowconvert(T, Dict(x.key => x.value for x in view(l.data, lo:hi))) : missing
    else
        return ArrowTypes.arrowconvert(T, Dict(x.key => x.value for x in view(l.data, lo:hi)))
    end
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: Map}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    offsets = compress(Z, comp, x.offsets.offsets)
    buffers = [validity, offsets]
    children = Compressed[]
    push!(children, compress(Z, comp, x.data))
    return Compressed{Z, A}(x, buffers, len, nc, children)
end

struct Struct{T, S} <: ArrowVector{T}
    validity::ValidityBitmap
    data::S # Tuple of ArrowVector
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(s::Struct) = (s.ℓ,)

function arrowvector(::StructType, ::Type{T}, ::Type{S}, x, de, meta; kw...) where {T, S}
    len = length(x)
    if S !== T
        validity = ValidityBitmap((y !== missing for y in x))
    else
        validity = ValidityBitmap(len)
    end
    data = Tuple(arrowvector(ToStruct(x, i), de, nothing; kw...) for i = 1:fieldcount(T))
    return Struct{S, typeof(data)}(validity, data, len, meta)
end

@propagate_inbounds function Base.getindex(s::Struct{T}, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    NT = Base.nonmissingtype(T)
    if ArrowTypes.structtype(NT) === ArrowTypes.NAMEDTUPLE
        if NT !== T
            return s.validity[i] ? NT(ntuple(j->s.data[j][i], fieldcount(NT))) : missing
        else
            return NT(ntuple(j->s.data[j][i], fieldcount(NT)))
        end
    elseif ArrowTypes.structtype(NT) === ArrowTypes.STRUCT
        if NT !== T
            return s.validity[i] ? NT(ntuple(j->s.data[j][i], fieldcount(NT))...) : missing
        else
            return NT(ntuple(j->s.data[j][i], fieldcount(NT))...)
        end
    end
end

@propagate_inbounds function Base.setindex!(s::Struct{T}, v::T, i::Integer) where {T}
    @boundscheck checkbounds(s, i)
    if v === missing
        @inbounds s.validity[i] = false
    else
        NT = Base.nonmissingtype(T)
        N = getN(NT)
        foreach(1:N) do j
            @inbounds s.data[j][i] = v[j]
        end
    end
    return v
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: Struct}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    buffers = [validity]
    children = Compressed[]
    for y in x.data
        push!(children, compress(Z, comp, y))
    end
    return Compressed{Z, A}(x, buffers, len, nc, children)
end

struct DenseUnion{T, S} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    arrow2::Vector{UInt8} # need to hold a reference to arrow memory blob
    typeIds::Vector{UInt8}
    offsets::Vector{Int32}
    data::S # Tuple of ArrowVector
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(s::DenseUnion) = size(s.typeIds)
nullcount(x::DenseUnion) = 0

arrowvector(U::Union, ::Type{S}, x, de, meta; denseunions::Bool=true, kw...) where {S} =
    arrowvector(denseunions ? DenseUnionVector(x) : SparseUnionVector(x), de, meta; denseunions=denseunions, kw...)

function arrowvector(::UnionType, ::Type{UT}, ::Type{S}, x, de, meta; kw...) where {UT <: UnionT, S}
    if unionmode(UT) == Meta.UnionMode.Dense
        x = x isa DenseUnionVector ? x.itr : x
        typeids, offsets, data = todense(UT, x)
        data2 = map(y -> arrowvector(y, de, nothing; kw...), data)
        return DenseUnion{UT, typeof(data2)}(UInt8[], UInt8[], typeids, offsets, data2, meta)
    else
        x = x isa SparseUnionVector ? x.itr : x
        typeids = sparsetypeids(UT, x)
        data3 = Tuple(arrowvector(ToSparseUnion(fieldtype(eltype(UT), i), x), de, nothing; kw...) for i = 1:fieldcount(eltype(UT)))
        return SparseUnion{UT, typeof(data3)}(UInt8[], typeids, data3, meta)
    end
end

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

mutable struct DictEncoding{T, A} <: ArrowVector{T}
    id::Int64
    data::A
    isOrdered::Bool
end

Base.size(d::DictEncoding) = size(d.data)

@propagate_inbounds function Base.getindex(d::DictEncoding{T}, i::Integer) where {T}
    @boundscheck checkbounds(d, i)
    return @inbounds ArrowTypes.arrowconvert(T, d.data[i])
end

# convenience wrapper to signal that an input column should be
# dict encoded when written to the arrow format
struct DictEncodeType{T} end
getT(::Type{DictEncodeType{T}}) where {T} = T

struct DictEncode{T, A} <: AbstractVector{DictEncodeType{T}}
    data::A
end

DictEncode(x::A) where {A} = DictEncode{eltype(A), A}(x)
Base.IndexStyle(::Type{<:DictEncode}) = Base.IndexLinear()
Base.size(x::DictEncode) = (length(x.data),)
Base.iterate(x::DictEncode, st...) = iterate(x.data, st...)
Base.getindex(x::DictEncode, i::Int) = getindex(x.data, i)
ArrowTypes.ArrowType(::Type{<:DictEncodeType}) = DictEncodedType()

struct DictEncoded{T, S, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    indices::Vector{S}
    encoding::DictEncoding{T, A}
    metadata::Union{Nothing, Dict{String, String}}
end

DictEncoded(b::Vector{UInt8}, v::ValidityBitmap, inds::Vector{S}, encoding::DictEncoding{T, A}, meta) where {S, T, A} =
    DictEncoded{T, S, A}(b, v, inds, encoding, meta)

Base.size(d::DictEncoded) = size(d.indices)

isdictencoded(d::DictEncoded) = true
isdictencoded(x) = false
isdictencoded(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = true

signedtype(::Type{UInt8}) = Int8
signedtype(::Type{UInt16}) = Int16
signedtype(::Type{UInt32}) = Int32
signedtype(::Type{UInt64}) = Int64

indtype(d::D) where {D <: DictEncoded} = indtype(D)
indtype(::Type{DictEncoded{T, S, A}}) where {T, S, A} = signedtype(S)
indtype(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = indtype(A)

getid(d::DictEncoded) = d.encoding.id
getid(c::Compressed{Z, A}) where {Z, A <: DictEncoded} = c.data.encoding.id

arrowvector(::DictEncodedType, ::Type{DictEncodeType{T}}, ::Type{DictEncodeType{S}}, x, de, meta; kw...) where {T, S} =
    arrowvector(DictEncodedType(), maybemissing(T), S, x, de, meta; kw...)

function arrowvector(::DictEncodedType, ::Type{T}, ::Type{S}, x, de, meta; dictencode::Bool=false, dictencodenested::Bool=false, kw...) where {T, S}
    @assert x isa DictEncode
    x = x.data
    len = length(x)
    if S !== T
        validity = ValidityBitmap((y !== missing for y in x))
    else
        validity = ValidityBitmap(len)
    end
    if x isa AbstractArray && DataAPI.refarray(x) !== x
        inds = copy(DataAPI.refarray(x))
        # adjust to "offset" instead of index
        for i = 1:length(inds)
            @inbounds inds[i] -= 1
        end
        pool = DataAPI.refpool(x)
        data = arrowvector(pool, de, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
        encoding = DictEncoding{S, typeof(data)}(0, data, false)
    else
        # need to encode ourselves
        y = PooledArray(x)
        inds = DataAPI.refarray(y)
        # adjust to "offset" instead of index
        for i = 1:length(inds)
            @inbounds inds[i] = inds[i] - 1
        end
        data = arrowvector(DataAPI.refpool(y), de, nothing; dictencode=dictencodenested, dictencodenested=dictencodenested, dictencoding=true, kw...)
        encoding = DictEncoding{S, typeof(data)}(0, data, false)
    end
    push!(de, encoding)
    return DictEncoded(UInt8[], validity, inds, encoding, data.metadata)
end

@propagate_inbounds function Base.getindex(d::DictEncoded, i::Integer)
    @boundscheck checkbounds(d, i)
    @inbounds valid = d.validity[i]
    !valid && return missing
    @inbounds idx = d.indices[i]
    return @inbounds d.encoding[idx + 1]
end

@propagate_inbounds function Base.setindex!(d::DictEncoded{T}, v, i::Integer) where {T}
    @boundscheck checkbounds(d, i)
    if v === missing
        @inbounds d.validity[i] = false
    else
        ix = findfirst(d.encoding.data, v)
        if ix === nothing
            push!(d.encoding.data, v)
            @inbounds d.indices[i] = length(d.encoding.data) - 1
        else
            @inbounds d.indices[i] = ix - 1
        end
    end
    return v
end

function Base.copy(x::DictEncoded{T, S}) where {T, S}
    pool = copy(x.encoding.data)
    valid = x.validity
    inds = x.indices
    if T !== S
        refs = Vector{S}(undef, length(inds))
        @inbounds for i = 1:length(inds)
            refs[i] = ifelse(valid[i], inds[i] + one(S), missing)
        end
    else
        refs = copy(inds)
        @inbounds for i = 1:length(inds)
            refs[i] = refs[i] + one(S)
        end
    end
    return PooledArray(PooledArrays.RefArray(refs), Dict{T, S}(val => i for (i, val) in enumerate(pool)), pool)
end

function compress(Z::Meta.CompressionType, comp, x::A) where {A <: DictEncoded}
    len = length(x)
    nc = nullcount(x)
    validity = compress(Z, comp, x.validity)
    inds = compress(Z, comp, x.indices)
    return Compressed{Z, A}(x, [validity, inds], len, nc, Compressed[])
end

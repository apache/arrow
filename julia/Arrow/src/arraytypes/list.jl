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

"""
    Arrow.List

An `ArrowVector` where each element is a variable sized list of some kind, like an `AbstractVector` or `AbstractString`.
"""
struct List{T, O, A} <: ArrowVector{T}
    arrow::Vector{UInt8} # need to hold a reference to arrow memory blob
    validity::ValidityBitmap
    offsets::Offsets{O}
    data::A
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(l::List) = (l.ℓ,)

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

# an AbstractVector version of Iterators.flatten
# code based on SentinelArrays.ChainedVector
struct ToList{T, stringtype, A, I} <: AbstractVector{T}
    data::Vector{A} # A is AbstractVector or AbstractString
    inds::Vector{I}
end

function ToList(input; largelists::Bool=false)
    AT = eltype(input)
    ST = Base.nonmissingtype(AT)
    stringtype = ArrowTypes.isstringtype(ST)
    T = stringtype ? UInt8 : eltype(ST)
    len = stringtype ? ncodeunits : length
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
    return ToList{T, stringtype, AT, I}(data, inds)
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

Base.@propagate_inbounds function Base.getindex(A::ToList{T, stringtype}, i::Integer) where {T, stringtype}
    @boundscheck checkbounds(A, i)
    chunk, ix = index(A, i)
    @inbounds x = A.data[chunk]
    return @inbounds stringtype ? codeunits(x)[ix] : x[ix]
end

Base.@propagate_inbounds function Base.setindex!(A::ToList{T, stringtype}, v, i::Integer) where {T, stringtype}
    @boundscheck checkbounds(A, i)
    chunk, ix = index(A, i)
    @inbounds x = A.data[chunk]
    if stringtype
        codeunits(x)[ix] = v
    else
        x[ix] = v
    end
    return v
end

# efficient iteration
@inline function Base.iterate(A::ToList{T, stringtype}) where {T, stringtype}
    length(A) == 0 && return nothing
    i = 1
    chunk = 2
    chunk_i = 1
    chunk_len = A.inds[chunk]
    while i > chunk_len
        chunk += 1
        chunk_len = A.inds[chunk]
    end
    val = A.data[chunk - 1]
    x = stringtype ? codeunits(val)[1] : val[1]
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

@inline function Base.iterate(A::ToList{T, stringtype}, (i, chunk, chunk_i, chunk_len, len)) where {T, stringtype}
    i > len && return nothing
    @inbounds val = A.data[chunk - 1]
    @inbounds x = stringtype ? codeunits(val)[chunk_i] : val[chunk_i]
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

arrowvector(::ListType, x::List, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::ListType, x, i, nl, fi, de, ded, meta; largelists::Bool=false, kw...)
    len = length(x)
    validity = ValidityBitmap(x)
    flat = ToList(x; largelists=largelists)
    offsets = Offsets(UInt8[], flat.inds)
    if eltype(flat) == UInt8 # binary or utf8string
        data = flat
    else
        data = arrowvector(flat, i, nl + 1, fi, de, ded, nothing; lareglists=largelists, kw...)
    end
    return List{eltype(x), eltype(flat.inds), typeof(data)}(UInt8[], validity, offsets, data, len, meta)
end

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

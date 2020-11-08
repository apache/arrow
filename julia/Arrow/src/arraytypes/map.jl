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
    Arrow.Map

An `ArrowVector` where each element is a "map" of some kind, like a `Dict`.
"""
struct Map{T, O, A} <: ArrowVector{T}
    validity::ValidityBitmap
    offsets::Offsets{O}
    data::A
    ℓ::Int
    metadata::Union{Nothing, Dict{String, String}}
end

Base.size(l::Map) = (l.ℓ,)

@propagate_inbounds function Base.getindex(l::Map{T}, i::Integer) where {T}
    @boundscheck checkbounds(l, i)
    @inbounds lo, hi = l.offsets[i]
    if Base.nonmissingtype(T) !== T
        return l.validity[i] ? ArrowTypes.arrowconvert(T, Dict(x.key => x.value for x in view(l.data, lo:hi))) : missing
    else
        return ArrowTypes.arrowconvert(T, Dict(x.key => x.value for x in view(l.data, lo:hi)))
    end
end

keyvalues(KT, ::Missing) = missing
keyvalues(KT, x::AbstractDict) = [KT(k, v) for (k, v) in pairs(x)]

arrowvector(::MapType, x::Map, i, nl, fi, de, ded, meta; kw...) = x

function arrowvector(::MapType, x, i, nl, fi, de, ded, meta; largelists::Bool=false, kw...)
    len = length(x)
    validity = ValidityBitmap(x)
    ET = eltype(x)
    DT = Base.nonmissingtype(ET)
    KT = KeyValue{keytype(DT), valtype(DT)}
    VT = Vector{KT}
    T = DT !== ET ? Union{Missing, VT} : VT
    flat = ToList(T[keyvalues(KT, y) for y in x]; largelists=largelists)
    offsets = Offsets(UInt8[], flat.inds)
    data = arrowvector(flat, i, nl + 1, fi, de, ded, nothing; lareglists=largelists, kw...)
    return Map{ET, eltype(flat.inds), typeof(data)}(validity, offsets, data, len, meta)
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

function makenodesbuffers!(col::Union{Map{T, O, A}, List{T, O, A}}, fieldnodes, fieldbuffers, bufferoffset, alignment) where {T, O, A}
    len = length(col)
    nc = nullcount(col)
    push!(fieldnodes, FieldNode(len, nc))
    @debug 1 "made field node: nodeidx = $(length(fieldnodes)), col = $(typeof(col)), len = $(fieldnodes[end].length), nc = $(fieldnodes[end].null_count)"
    # validity bitmap
    blen = nc == 0 ? 0 : bitpackedbytes(len, alignment)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    # adjust buffer offset, make array buffer
    bufferoffset += blen
    blen = sizeof(O) * (len + 1)
    push!(fieldbuffers, Buffer(bufferoffset, blen))
    @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
    bufferoffset += padding(blen, alignment)
    if eltype(A) == UInt8
        blen = length(col.data)
        push!(fieldbuffers, Buffer(bufferoffset, blen))
        @debug 1 "made field buffer: bufferidx = $(length(fieldbuffers)), offset = $(fieldbuffers[end].offset), len = $(fieldbuffers[end].length), padded = $(padding(fieldbuffers[end].length, alignment))"
        bufferoffset += padding(blen, alignment)
    else
        bufferoffset = makenodesbuffers!(col.data, fieldnodes, fieldbuffers, bufferoffset, alignment)
    end
    return bufferoffset
end

function writebuffer(io, col::Union{Map{T, O, A}, List{T, O, A}}, alignment) where {T, O, A}
    @debug 1 "writebuffer: col = $(typeof(col))"
    @debug 2 col
    writebitmap(io, col, alignment)
    # write offsets
    n = writearray(io, O, col.offsets.offsets)
    @debug 1 "writing array: col = $(typeof(col.offsets.offsets)), n = $n, padded = $(padding(n, alignment))"
    writezeros(io, paddinglength(n, alignment))
    # write values array
    if eltype(A) == UInt8
        n = writearray(io, UInt8, col.data)
        @debug 1 "writing array: col = $(typeof(col.data)), n = $n, padded = $(padding(n, alignment))"
        writezeros(io, paddinglength(n, alignment))
    else
        writebuffer(io, col.data, alignment)
    end
    return
end

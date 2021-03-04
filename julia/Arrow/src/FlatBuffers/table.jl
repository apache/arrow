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
Table

The object containing the flatbuffer and positional information specific to the table.
The `vtable` containing the offsets for specific members precedes `pos`.
The actual values in the table follow `pos` offset and size of the vtable.

- `bytes::Vector{UInt8}`: the flatbuffer itself
- `pos::Integer`:  the base position in `bytes` of the table
"""
abstract type Table end
abstract type Struct end

const TableOrStruct = Union{Table, Struct}

bytes(x::TableOrStruct) = getfield(x, :bytes)
pos(x::TableOrStruct) = getfield(x, :pos)

(::Type{T})(b::Builder) where {T <: TableOrStruct} = T(b.bytes[b.head+1:end], get(b, b.head, Int32))

getrootas(::Type{T}, bytes::Vector{UInt8}, offset) where {T <: Table} = init(T, bytes, offset + readbuffer(bytes, offset, UOffsetT))
init(::Type{T}, bytes::Vector{UInt8}, pos::Integer) where {T <: TableOrStruct} = T(bytes, pos)

const TableOrBuilder = Union{Table, Struct, Builder}

Base.get(t::TableOrBuilder, pos, ::Type{T}) where {T} = readbuffer(bytes(t), pos, T)
Base.get(t::TableOrBuilder, pos, ::Type{T}) where {T <: Enum} = T(get(t, pos, basetype(T)))

"""
`offset` provides access into the Table's vtable.

Deprecated fields are ignored by checking against the vtable's length.
"""
function offset(t::Table, vtableoffset)
    vtable = pos(t) - get(t, pos(t), SOffsetT)
    return vtableoffset < get(t, vtable, VOffsetT) ? get(t, vtable + vtableoffset, VOffsetT) : VOffsetT(0)
end

"`indirect` retrieves the relative offset stored at `offset`."
indirect(t::Table, off) = off + get(t, off, UOffsetT)

getvalue(t, o, ::Type{Nothing}) = nothing
getvalue(t, o, ::Type{T}) where {T <: Scalar} = get(t, pos(t) + o, T)
getvalue(t, o, ::Type{T}) where {T <: Enum} = T(get(t, pos(t) + o, enumtype(T)))

function Base.String(t::Table, off)
    off += get(t, off, UOffsetT)
    start = off + sizeof(UOffsetT)
    len = get(t, off, UOffsetT)
    return unsafe_string(pointer(bytes(t), start + 1), len)
end

function bytevector(t::Table, off)
    off += get(t, off, UOffsetT)
    start = off + sizeof(UOffsetT)
    len = get(t, off, UOffsetT)
    return view(bytes(t), (start + 1):(start + len + 1))
end

"""
`vectorlen` retrieves the length of the vector whose offset is stored at
`off` in this object.
"""
function vectorlen(t::Table, off)
    off += pos(t)
    off += get(t, off, UOffsetT)
    return Int(get(t, off, UOffsetT))
end

"""
`vector` retrieves the start of data of the vector whose offset is stored
at `off` in this object.
"""
function vector(t::Table, off)
    off += pos(t)
    x = off + get(t, off, UOffsetT)
    # data starts after metadata containing the vector length
    return x + sizeof(UOffsetT)
end

struct Array{T, S, TT} <: AbstractVector{T}
    _tab::TT
    pos::Int64
    data::Vector{S}
end

function Array{T}(t::Table, off) where {T}
    a = vector(t, off)
    S = T <: Table ? UOffsetT : T <: Struct ? NTuple{structsizeof(T), UInt8} : T
    ptr = convert(Ptr{S}, pointer(bytes(t), pos(t) + a + 1))
    data = unsafe_wrap(Base.Array, ptr, vectorlen(t, off))
    return Array{T, S, typeof(t)}(t, a, data)
end

function structsizeof end

Base.IndexStyle(::Type{<:Array}) = Base.IndexLinear()
Base.size(x::Array) = size(x.data)
Base.@propagate_inbounds function Base.getindex(A::Array{T, S}, i::Integer) where {T, S}
    if T === S
        return A.data[i]
    elseif T <: Struct
        return init(T, bytes(A._tab), A.pos + (i - 1) * structsizeof(T))
    else # T isa Table
        return init(T, bytes(A._tab), indirect(A._tab, A.pos + (i - 1) * 4))
    end
end

Base.@propagate_inbounds function Base.setindex!(A::Array{T, S}, v, i::Integer) where {T, S}
    if T === S
        return setindex!(A.data, v, i)
    else
        error("setindex! not supported for reference/table types")
    end
end

function union(t::Table, off)
    off += pos(t)
    return off + get(t, off, UOffsetT)
end

function union!(t::Table, t2::Table, off)
    off += pos(t)
    t2.pos = off + get(t, off, UOffsetT)
    t2.bytes = bytes(t)
    return
end

"""
GetVOffsetTSlot retrieves the VOffsetT that the given vtable location
points to. If the vtable value is zero, the default value `d`
will be returned.
"""
function getoffsetslot(t::Table, slot, d)
    off = offset(t, slot)
    if off == 0
        return d
    end
    return off
end

"""
`getslot` retrieves the `T` that the given vtable location
points to. If the vtable value is zero, the default value `d`
will be returned.
"""
function getslot(t::Table, slot, d::T) where {T}
    off = offset(t, slot)
    if off == 0
        return d
    end

    return get(t, pos(t) + off, T)
end

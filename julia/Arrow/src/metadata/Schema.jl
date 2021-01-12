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

FlatBuffers.@scopedenum MetadataVersion::Int16 V1 V2 V3 V4 V5

struct Null <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Null) = ()

nullStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
nullEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Struct <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Struct) = ()

structStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
structEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct List <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::List) = ()

listStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
listEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct LargeList <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::LargeList) = ()

largeListStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
largeListEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct FixedSizeList <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::FixedSizeList) = (:listSize,)

function Base.getproperty(x::FixedSizeList, field::Symbol)
    if field === :listSize
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
        return Int32(0)
    end
    return nothing
end

fixedSizeListStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
fixedSizeListAddListSize(b::FlatBuffers.Builder, listSize::Int32) = FlatBuffers.prependslot!(b, 0, listSize, 0)
fixedSizeListEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Map <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Map) = (:keysSorted,)

function Base.getproperty(x::Map, field::Symbol)
    if field === :keysSorted
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Base.Bool)
    end
    return nothing
end

mapStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
mapAddKeysSorted(b::FlatBuffers.Builder, keyssorted::Base.Bool) = FlatBuffers.prependslot!(b, 0, keyssorted, 0)
mapEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum UnionMode::Int16 Sparse Dense

struct Union <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Union) = (:mode, :typeIds)

function Base.getproperty(x::Union, field::Symbol)
    if field === :mode
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), UnionMode)
        return UnionMode.Sparse
    elseif field === :typeIds
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.Array{Int32}(x, o)
    end
    return nothing
end

unionStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 2)
unionAddMode(b::FlatBuffers.Builder, mode::UnionMode) = FlatBuffers.prependslot!(b, 0, mode, 0)
unionAddTypeIds(b::FlatBuffers.Builder, typeIds::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, typeIds, 0)
unionStartTypeIdsVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 4, numelems, 4)
unionEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Int <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Int) = (:bitWidth, :is_signed)

function Base.getproperty(x::Int, field::Symbol)
    if field === :bitWidth
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
    elseif field === :is_signed
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Base.Bool)
        return false
    end
    return nothing
end

intStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 2)
intAddBitWidth(b::FlatBuffers.Builder, bitwidth::Int32) = FlatBuffers.prependslot!(b, 0, bitwidth, 0)
intAddIsSigned(b::FlatBuffers.Builder, issigned::Base.Bool) = FlatBuffers.prependslot!(b, 1, issigned, 0)
intEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum Precision::Int16 HALF SINGLE DOUBLE

struct FloatingPoint <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::FloatingPoint) = (:precision,)

function Base.getproperty(x::FloatingPoint, field::Symbol)
    if field === :precision
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Precision)
        return Precision.HALF
    end
    return nothing
end

floatingPointStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
floatingPointAddPrecision(b::FlatBuffers.Builder, precision::Precision) = FlatBuffers.prependslot!(b, 0, precision, 0)
floatingPointEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Utf8 <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Utf8) = ()

utf8Start(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
utf8End(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Binary <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Binary) = ()

binaryStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
binaryEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct LargeUtf8 <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::LargeUtf8) = ()

largUtf8Start(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
largUtf8End(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct LargeBinary <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::LargeBinary) = ()

largeBinaryStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
largeBinaryEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct FixedSizeBinary <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::FixedSizeBinary) = (:byteWidth,)

function Base.getproperty(x::FixedSizeBinary, field::Symbol)
    if field === :byteWidth
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
    end
    return nothing
end

fixedSizeBinaryStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
fixedSizeBinaryAddByteWidth(b::FlatBuffers.Builder, bytewidth::Int32) = FlatBuffers.prependslot!(b, 0, bytewidth, 0)
fixedSizeBinaryEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Bool <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Bool) = ()

boolStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 0)
boolEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Decimal <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Decimal) = (:precision, :scale, :bitWidth)

function Base.getproperty(x::Decimal, field::Symbol)
    if field === :precision
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
        return Int32(0)
    elseif field === :scale
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
        return Int32(0)
    elseif field === :bitWidth
        o = FlatBuffers.offset(x, 8)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
        return Int32(128)
    end
    return nothing
end

decimalStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 3)
decimalAddPrecision(b::FlatBuffers.Builder, precision::Int32) = FlatBuffers.prependslot!(b, 0, precision, 0)
decimalAddScale(b::FlatBuffers.Builder, scale::Int32) = FlatBuffers.prependslot!(b, 1, scale, 0)
decimalAddBitWidth(b::FlatBuffers.Builder, bitWidth::Int32) = FlatBuffers.prependslot!(b, 2, bitWidth, Int32(128))
decimalEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum DateUnit::Int16 DAY MILLISECOND

struct Date <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Date) = (:unit,)

function Base.getproperty(x::Date, field::Symbol)
    if field === :unit
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), DateUnit)
        return DateUnit.MILLISECOND
    end
    return nothing
end

dateStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
dateAddUnit(b::FlatBuffers.Builder, unit::DateUnit) = FlatBuffers.prependslot!(b, 0, unit, 1)
dateEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum TimeUnit::Int16 SECOND MILLISECOND MICROSECOND NANOSECOND

struct Time <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Time) = (:unit, :bitWidth)

function Base.getproperty(x::Time, field::Symbol)
    if field === :unit
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), TimeUnit)
        return TimeUnit.MILLISECOND
    elseif field === :bitWidth
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int32)
        return 32
    end
    return nothing
end

timeStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 2)
timeAddUnit(b::FlatBuffers.Builder, unit::TimeUnit) = FlatBuffers.prependslot!(b, 0, unit, 1)
timeAddBitWidth(b::FlatBuffers.Builder, bitwidth::Int32) = FlatBuffers.prependslot!(b, 1, bitwidth, 32)
timeEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Timestamp <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Timestamp) = (:unit, :timezone)

function Base.getproperty(x::Timestamp, field::Symbol)
    if field === :unit
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), TimeUnit)
        return TimeUnit.SECOND
    elseif field === :timezone
        o = FlatBuffers.offset(x, 6)
        o != 0 && return String(x, o + FlatBuffers.pos(x))
    end
    return nothing
end

timestampStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 2)
timestampAddUnit(b::FlatBuffers.Builder, unit::TimeUnit) = FlatBuffers.prependslot!(b, 0, unit, 0)
timestampAddTimezone(b::FlatBuffers.Builder, timezone::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, timezone, 0)
timestampEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum IntervalUnit::Int16 YEAR_MONTH DAY_TIME

struct Interval <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Interval) = (:unit,)

function Base.getproperty(x::Interval, field::Symbol)
    if field === :unit
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), IntervalUnit)
        return IntervalUnit.YEAR_MONTH
    end
    return nothing
end

intervalStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
intervalAddUnit(b::FlatBuffers.Builder, unit::IntervalUnit) = FlatBuffers.prependslot!(b, 0, unit, 0)
intervalEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Duration <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Duration) = (:unit,)

function Base.getproperty(x::Duration, field::Symbol)
    if field === :unit
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), TimeUnit)
        return TimeUnit.MILLISECOND
    end
    return nothing
end

durationStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 1)
durationAddUnit(b::FlatBuffers.Builder, unit::TimeUnit) = FlatBuffers.prependslot!(b, 0, unit, 1)
durationEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

function Type(b::UInt8)
    b == 1 && return Null
    b == 2 && return Int
    b == 3 && return FloatingPoint
    b == 4 && return Binary
    b == 5 && return Utf8
    b == 6 && return Bool
    b == 7 && return Decimal
    b == 8 && return Date
    b == 9 && return Time
    b == 10 && return Timestamp
    b == 11 && return Interval
    b == 12 && return List
    b == 13 && return Struct
    b == 14 && return Union
    b == 15 && return FixedSizeBinary
    b == 16 && return FixedSizeList
    b == 17 && return Map
    b == 18 && return Duration
    b == 19 && return LargeBinary
    b == 20 && return LargeUtf8
    b == 21 && return LargeList
    return nothing
end

function Type(::Base.Type{T})::Int16 where {T}
    T == Null && return 1
    T == Int && return 2
    T == FloatingPoint && return 3
    T == Binary && return 4
    T == Utf8 && return 5
    T == Bool && return 6
    T == Decimal && return 7
    T == Date && return 8
    T == Time && return 9
    T == Timestamp && return 10
    T == Interval && return 11
    T == List && return 12
    T == Struct && return 13
    T == Union && return 14
    T == FixedSizeBinary && return 15
    T == FixedSizeList && return 16
    T == Map && return 17
    T == Duration && return 18
    T == LargeBinary && return 19
    T == LargeUtf8 && return 20
    T == LargeList && return 21
    return 0
end

struct KeyValue <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::KeyValue) = (:key, :value)

function Base.getproperty(x::KeyValue, field::Symbol)
    if field === :key
        o = FlatBuffers.offset(x, 4)
        o != 0 && return String(x, o + FlatBuffers.pos(x))
    elseif field === :value
        o = FlatBuffers.offset(x, 6)
        o != 0 && return String(x, o + FlatBuffers.pos(x))
    end
    return nothing
end

keyValueStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 2)
keyValueAddKey(b::FlatBuffers.Builder, key::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 0, key, 0)
keyValueAddValue(b::FlatBuffers.Builder, value::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, value, 0)
keyValueEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum DictionaryKind::Int16 DenseArray

struct DictionaryEncoding <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::DictionaryEncoding) = (:id, :indexType, :isOrdered, :dictionaryKind)

function Base.getproperty(x::DictionaryEncoding, field::Symbol)
    if field === :id
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Int64)
        return Int64(0)
    elseif field === :indexType
        o = FlatBuffers.offset(x, 6)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(Int, FlatBuffers.bytes(x), y)
        end
    elseif field === :isOrdered
        o = FlatBuffers.offset(x, 8)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Base.Bool)
        return false
    elseif field === :dictionaryKind
        o = FlatBuffers.offset(x, 10)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), DictionaryKind)
    end
    return nothing
end

dictionaryEncodingStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 3)
dictionaryEncodingAddId(b::FlatBuffers.Builder, id::Int64) = FlatBuffers.prependslot!(b, 0, id, 0)
dictionaryEncodingAddIndexType(b::FlatBuffers.Builder, indextype::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, indextype, 0)
dictionaryEncodingAddIsOrdered(b::FlatBuffers.Builder, isordered::Base.Bool) = FlatBuffers.prependslot!(b, 1, isordered, 0)
dictionaryEncodingEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

struct Field <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Field) = (:name, :nullable, :type, :dictionary, :children, :custom_metadata)

function Base.getproperty(x::Field, field::Symbol)
    if field === :name
        o = FlatBuffers.offset(x, 4)
        o != 0 && return String(x, o + FlatBuffers.pos(x))
    elseif field === :nullable
        o = FlatBuffers.offset(x, 6)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Base.Bool)
        return false
    elseif field === :type
        o = FlatBuffers.offset(x, 8)
        if o != 0
            T = Type(FlatBuffers.get(x, o + FlatBuffers.pos(x), UInt8))
            o = FlatBuffers.offset(x, 10)
            pos = FlatBuffers.union(x, o)
            if o != 0
                return FlatBuffers.init(T, FlatBuffers.bytes(x), pos)
            end
        end
    elseif field === :dictionary
        o = FlatBuffers.offset(x, 12)
        if o != 0
            y = FlatBuffers.indirect(x, o + FlatBuffers.pos(x))
            return FlatBuffers.init(DictionaryEncoding, FlatBuffers.bytes(x), y)
        end
    elseif field === :children
        o = FlatBuffers.offset(x, 14)
        if o != 0
            return FlatBuffers.Array{Field}(x, o)
        end
    elseif field === :custom_metadata
        o = FlatBuffers.offset(x, 16)
        if o != 0
            return FlatBuffers.Array{KeyValue}(x, o)
        end
    end
    return nothing
end

fieldStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 7)
fieldAddName(b::FlatBuffers.Builder, name::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 0, name, 0)
fieldAddNullable(b::FlatBuffers.Builder, nullable::Base.Bool) = FlatBuffers.prependslot!(b, 1, nullable, false)
fieldAddTypeType(b::FlatBuffers.Builder, ::Core.Type{T}) where {T} = FlatBuffers.prependslot!(b, 2, Type(T), 0)
fieldAddType(b::FlatBuffers.Builder, type::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 3, type, 0)
fieldAddDictionary(b::FlatBuffers.Builder, dictionary::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 4, dictionary, 0)
fieldAddChildren(b::FlatBuffers.Builder, children::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 5, children, 0)
fieldStartChildrenVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 4, numelems, 4)
fieldAddCustomMetadata(b::FlatBuffers.Builder, custommetadata::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 6, custommetadata, 0)
fieldStartCustomMetadataVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 4, numelems, 4)
fieldEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

FlatBuffers.@scopedenum Endianness::Int16 Little Big

struct Buffer <: FlatBuffers.Struct
    bytes::Vector{UInt8}
    pos::Base.Int
end

FlatBuffers.structsizeof(::Base.Type{Buffer}) = 16

Base.propertynames(x::Buffer) = (:offset, :length)

function Base.getproperty(x::Buffer, field::Symbol)
    if field === :offset
        return FlatBuffers.get(x, FlatBuffers.pos(x), Int64)
    elseif field === :length
        return FlatBuffers.get(x, FlatBuffers.pos(x) + 8, Int64)
    end
    return nothing
end

function createBuffer(b::FlatBuffers.Builder, offset::Int64, length::Int64)
    FlatBuffers.prep!(b, 8, 16)
    prepend!(b, length)
    prepend!(b, offset)
    return FlatBuffers.offset(b)
end

struct Schema <: FlatBuffers.Table
    bytes::Vector{UInt8}
    pos::Base.Int
end

Base.propertynames(x::Schema) = (:endianness, :fields, :custom_metadata)

function Base.getproperty(x::Schema, field::Symbol)
    if field === :endianness
        o = FlatBuffers.offset(x, 4)
        o != 0 && return FlatBuffers.get(x, o + FlatBuffers.pos(x), Endianness)
    elseif field === :fields
        o = FlatBuffers.offset(x, 6)
        if o != 0
            return FlatBuffers.Array{Field}(x, o)
        end
    elseif field === :custom_metadata
        o = FlatBuffers.offset(x, 8)
        if o != 0
            return FlatBuffers.Array{KeyValue}(x, o)
        end
    end
    return nothing
end

schemaStart(b::FlatBuffers.Builder) = FlatBuffers.startobject!(b, 3)
schemaAddEndianness(b::FlatBuffers.Builder, endianness::Endianness) = FlatBuffers.prependslot!(b, 0, endianness, 0)
schemaAddFields(b::FlatBuffers.Builder, fields::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 1, fields, 0)
schemaStartFieldsVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 4, numelems, 4)
schemaAddCustomMetadata(b::FlatBuffers.Builder, custommetadata::FlatBuffers.UOffsetT) = FlatBuffers.prependoffsetslot!(b, 2, custommetadata, 0)
schemaStartCustomMetadataVector(b::FlatBuffers.Builder, numelems) = FlatBuffers.startvector!(b, 4, numelems, 4)
schemaEnd(b::FlatBuffers.Builder) = FlatBuffers.endobject!(b)

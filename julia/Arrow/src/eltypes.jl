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
Given a flatbuffers metadata type definition (a Field instance from Schema.fbs),
translate to the appropriate Julia storage eltype
"""
function juliaeltype end

finaljuliatype(T) = T
finaljuliatype(::Type{Missing}) = Missing
finaljuliatype(::Type{Union{T, Missing}}) where {T} = Union{Missing, finaljuliatype(T)}

"""
Given a FlatBuffers.Builder and a Julia column or column eltype,
Write the field.type flatbuffer definition of the eltype
"""
function arrowtype end

arrowtype(b, col::AbstractVector{T}) where {T} = arrowtype(b, maybemissing(T))
arrowtype(b, col::DictEncoded) = arrowtype(b, col.encoding.data)
arrowtype(b, col::Compressed) = arrowtype(b, col.data)

function juliaeltype(f::Meta.Field, ::Nothing, convert::Bool)
    T = juliaeltype(f, convert)
    return convert ? finaljuliatype(T) : T
end

function juliaeltype(f::Meta.Field, meta::Dict{String, String}, convert::Bool)
    TT = juliaeltype(f, convert)
    !convert && return TT
    T = finaljuliatype(TT)
    TTT = ArrowTypes.extensiontype(f, meta)
    return something(TTT, T)
end

function juliaeltype(f::Meta.Field, convert::Bool)
    T = juliaeltype(f, f.type, convert)
    return (f.nullable ? Union{T, Missing} : T)
end

juliaeltype(f::Meta.Field, ::Meta.Null, convert) = Missing

function arrowtype(b, ::Type{Missing})
    Meta.nullStart(b)
    return Meta.Null, Meta.nullEnd(b), nothing
end

function juliaeltype(f::Meta.Field, int::Meta.Int, convert)
    if int.is_signed
        if int.bitWidth == 8
            Int8
        elseif int.bitWidth == 16
            Int16
        elseif int.bitWidth == 32
            Int32
        elseif int.bitWidth == 64
            Int64
        elseif int.bitWidth == 128
            Int128
        else
            error("$int is not valid arrow type metadata")
        end
    else
        if int.bitWidth == 8
            UInt8
        elseif int.bitWidth == 16
            UInt16
        elseif int.bitWidth == 32
            UInt32
        elseif int.bitWidth == 64
            UInt64
        elseif int.bitWidth == 128
            UInt128
        else
            error("$int is not valid arrow type metadata")
        end
    end
end

function arrowtype(b, ::Type{T}) where {T <: Integer}
    Meta.intStart(b)
    Meta.intAddBitWidth(b, Int32(8 * sizeof(T)))
    Meta.intAddIsSigned(b, T <: Signed)
    return Meta.Int, Meta.intEnd(b), nothing
end

# primitive types
function juliaeltype(f::Meta.Field, fp::Meta.FloatingPoint, convert)
    if fp.precision == Meta.Precision.HALF
        Float16
    elseif fp.precision == Meta.Precision.SINGLE
        Float32
    elseif fp.precision == Meta.Precision.DOUBLE
        Float64
    end
end

function arrowtype(b, ::Type{T}) where {T <: AbstractFloat}
    Meta.floatingPointStart(b)
    Meta.floatingPointAddPrecision(b, T === Float16 ? Meta.Precision.HALF : T === Float32 ? Meta.Precision.SINGLE : Meta.Precision.DOUBLE)
    return Meta.FloatingPoint, Meta.floatingPointEnd(b), nothing
end

juliaeltype(f::Meta.Field, b::Union{Meta.Utf8, Meta.LargeUtf8}, convert) = String

datasizeof(x) = sizeof(x)
datasizeof(x::AbstractVector) = sum(datasizeof, x)

juliaeltype(f::Meta.Field, b::Union{Meta.Binary, Meta.LargeBinary}, convert) = Vector{UInt8}

juliaeltype(f::Meta.Field, x::Meta.FixedSizeBinary, convert) = NTuple{Int(x.byteWidth), UInt8}

# arggh!
Base.write(io::IO, x::NTuple{N, T}) where {N, T} = sum(y -> Base.write(io, y), x)

juliaeltype(f::Meta.Field, x::Meta.Bool, convert) = Bool

function arrowtype(b, ::Type{Bool})
    Meta.boolStart(b)
    return Meta.Bool, Meta.boolEnd(b), nothing
end

struct Decimal{P, S, T}
    value::T # only Int128 or Int256
end

Base.zero(::Type{Decimal{P, S, T}}) where {P, S, T} = Decimal{P, S, T}(T(0))
==(a::Decimal{P, S, T}, b::Decimal{P, S, T}) where {P, S, T} = ==(a.value, b.value)
Base.isequal(a::Decimal{P, S, T}, b::Decimal{P, S, T}) where {P, S, T} = isequal(a.value, b.value)

function juliaeltype(f::Meta.Field, x::Meta.Decimal, convert)
    return Decimal{x.precision, x.scale, x.bitWidth == 256 ? Int256 : Int128}
end

ArrowTypes.ArrowType(::Type{<:Decimal}) = PrimitiveType()

function arrowtype(b, ::Type{Decimal{P, S, T}}) where {P, S, T}
    Meta.decimalStart(b)
    Meta.decimalAddPrecision(b, Int32(P))
    Meta.decimalAddScale(b, Int32(S))
    Meta.decimalAddBitWidth(b, Int32(T == Int256 ? 256 : 128))
    return Meta.Decimal, Meta.decimalEnd(b), nothing
end

Base.write(io::IO, x::Decimal) = Base.write(io, x.value)

abstract type ArrowTimeType end
Base.write(io::IO, x::ArrowTimeType) = Base.write(io, x.x)
ArrowTypes.ArrowType(::Type{<:ArrowTimeType}) = PrimitiveType()

struct Date{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Date{U, T}}) where {U, T} = Date{U, T}(T(0))
storagetype(::Type{Date{U, T}}) where {U, T} = T
bitwidth(x::Meta.DateUnit) = x == Meta.DateUnit.DAY ? Int32 : Int64
Date{Meta.DateUnit.DAY}(days) = Date{Meta.DateUnit.DAY, Int32}(Int32(days))
Date{Meta.DateUnit.MILLISECOND}(ms) = Date{Meta.DateUnit.MILLISECOND, Int64}(Int64(ms))
const DATE = Date{Meta.DateUnit.DAY, Int32}

juliaeltype(f::Meta.Field, x::Meta.Date, convert) = Date{x.unit, bitwidth(x.unit)}
finaljuliatype(::Type{Date{Meta.DateUnit.DAY, Int32}}) = Dates.Date
Base.convert(::Type{Dates.Date}, x::Date{Meta.DateUnit.DAY, Int32}) = Dates.Date(Dates.UTD(Int64(x.x + UNIX_EPOCH_DATE)))
finaljuliatype(::Type{Date{Meta.DateUnit.MILLISECOND, Int64}}) = Dates.DateTime
Base.convert(::Type{Dates.DateTime}, x::Date{Meta.DateUnit.MILLISECOND, Int64}) = Dates.DateTime(Dates.UTM(Int64(x.x + UNIX_EPOCH_DATETIME)))

function arrowtype(b, ::Type{Date{U, T}}) where {U, T}
    Meta.dateStart(b)
    Meta.dateAddUnit(b, U)
    return Meta.Date, Meta.dateEnd(b), nothing
end

const UNIX_EPOCH_DATE = Dates.value(Dates.Date(1970))
Base.convert(::Type{Date{Meta.DateUnit.DAY, Int32}}, x::Dates.Date) = Date{Meta.DateUnit.DAY, Int32}(Int32(Dates.value(x) - UNIX_EPOCH_DATE))

const UNIX_EPOCH_DATETIME = Dates.value(Dates.DateTime(1970))
Base.convert(::Type{Date{Meta.DateUnit.MILLISECOND, Int64}}, x::Dates.DateTime) = Date{Meta.DateUnit.MILLISECOND, Int64}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

struct Time{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Time{U, T}}) where {U, T} = Time{U, T}(T(0))
const TIME = Time{Meta.TimeUnit.NANOSECOND, Int64}

bitwidth(x::Meta.TimeUnit) = x == Meta.TimeUnit.SECOND || x == Meta.TimeUnit.MILLISECOND ? Int32 : Int64
Time{U}(x) where {U <: Meta.TimeUnit} = Time{U, bitwidth(U)}(bitwidth(U)(x))
storagetype(::Type{Time{U, T}}) where {U, T} = T
juliaeltype(f::Meta.Field, x::Meta.Time, convert) = Time{x.unit, bitwidth(x.unit)}
finaljuliatype(::Type{<:Time}) = Dates.Time
periodtype(U::Meta.TimeUnit) = U === Meta.TimeUnit.SECOND ? Dates.Second :
                               U === Meta.TimeUnit.MILLISECOND ? Dates.Millisecond :
                               U === Meta.TimeUnit.MICROSECOND ? Dates.Microsecond : Dates.Nanosecond
Base.convert(::Type{Dates.Time}, x::Time{U, T}) where {U, T} = Dates.Time(Dates.Nanosecond(Dates.tons(periodtype(U)(x.x))))

function arrowtype(b, ::Type{Time{U, T}}) where {U, T}
    Meta.timeStart(b)
    Meta.timeAddUnit(b, U)
    Meta.timeAddBitWidth(b, Int32(8 * sizeof(T)))
    return Meta.Time, Meta.timeEnd(b), nothing
end

Base.convert(::Type{Time{Meta.TimeUnit.NANOSECOND, Int64}}, x::Dates.Time) = Time{Meta.TimeUnit.NANOSECOND, Int64}(Dates.value(x))

struct Timestamp{U, TZ} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Timestamp{U, T}}) where {U, T} = Timestamp{U, T}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Timestamp, convert)
    return Timestamp{x.unit, x.timezone === nothing ? nothing : Symbol(x.timezone)}
end

const DATETIME = Timestamp{Meta.TimeUnit.MILLISECOND, nothing}

finaljuliatype(::Type{Timestamp{U, TZ}}) where {U, TZ} = ZonedDateTime
finaljuliatype(::Type{Timestamp{U, nothing}}) where {U} = DateTime
Base.convert(::Type{ZonedDateTime}, x::Timestamp{U, TZ}) where {U, TZ} =
    ZonedDateTime(Dates.DateTime(Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME))), TimeZone(String(TZ)))
Base.convert(::Type{DateTime}, x::Timestamp{U, nothing}) where {U} =
    Dates.DateTime(Dates.UTM(Int64(Dates.toms(periodtype(U)(x.x)) + UNIX_EPOCH_DATETIME)))
Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND, TZ}}, x::ZonedDateTime) where {TZ} =
    Timestamp{Meta.TimeUnit.MILLISECOND, TZ}(Int64(Dates.value(DateTime(x)) - UNIX_EPOCH_DATETIME))
Base.convert(::Type{Timestamp{Meta.TimeUnit.MILLISECOND, nothing}}, x::DateTime) =
    Timestamp{Meta.TimeUnit.MILLISECOND, nothing}(Int64(Dates.value(x) - UNIX_EPOCH_DATETIME))

function arrowtype(b, ::Type{Timestamp{U, TZ}}) where {U, TZ}
    tz = TZ !== nothing ? FlatBuffers.createstring!(b, String(TZ)) : FlatBuffers.UOffsetT(0)
    Meta.timestampStart(b)
    Meta.timestampAddUnit(b, U)
    Meta.timestampAddTimezone(b, tz)
    return Meta.Timestamp, Meta.timestampEnd(b), nothing
end

struct Interval{U, T} <: ArrowTimeType
    x::T
end

Base.zero(::Type{Interval{U, T}}) where {U, T} = Interval{U, T}(T(0))

bitwidth(x::Meta.IntervalUnit) = x == Meta.IntervalUnit.YEAR_MONTH ? Int32 : Int64
Interval{Meta.IntervalUnit.YEAR_MONTH}(x) = Interval{Meta.IntervalUnit.YEAR_MONTH, Int32}(Int32(x))
Interval{Meta.IntervalUnit.DAY_TIME}(x) = Interval{Meta.IntervalUnit.DAY_TIME, Int64}(Int64(x))

function juliaeltype(f::Meta.Field, x::Meta.Interval, convert)
    return Interval{x.unit, bitwidth(x.unit)}
end

function arrowtype(b, ::Type{Interval{U, T}}) where {U, T}
    Meta.intervalStart(b)
    Meta.intervalAddUnit(b, U)
    return Meta.Interval, Meta.intervalEnd(b), nothing
end

struct Duration{U} <: ArrowTimeType
    x::Int64
end

Base.zero(::Type{Duration{U}}) where {U} = Duration{U}(Int64(0))

function juliaeltype(f::Meta.Field, x::Meta.Duration, convert)
    return Duration{x.unit}
end

finaljuliatype(::Type{Duration{U}}) where {U} = periodtype(U)
Base.convert(::Type{P}, x::Duration{U}) where {P <: Dates.Period, U} = P(periodtype(U)(x.x))

function arrowtype(b, ::Type{Duration{U}}) where {U}
    Meta.durationStart(b)
    Meta.durationAddUnit(b, U)
    return Meta.Duration, Meta.durationEnd(b), nothing
end

arrowperiodtype(P) = Meta.TimeUnit.SECOND
arrowperiodtype(::Type{Dates.Millisecond}) = Meta.TimeUnit.MILLISECOND
arrowperiodtype(::Type{Dates.Microsecond}) = Meta.TimeUnit.MICROSECOND
arrowperiodtype(::Type{Dates.Nanosecond}) = Meta.TimeUnit.NANOSECOND

Base.convert(::Type{Duration{U}}, x::Dates.Period) where {U} = Duration{U}(Dates.value(periodtype(U)(x)))

# nested types; call juliaeltype recursively on nested children
function juliaeltype(f::Meta.Field, list::Union{Meta.List, Meta.LargeList}, convert)
    return Vector{juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)}
end

# arrowtype will call fieldoffset recursively for children
function arrowtype(b, x::List{T, O, A}) where {T, O, A}
    if eltype(A) == UInt8
        if T <: AbstractString || T <: Union{AbstractString, Missing}
            if O == Int32
                Meta.utf8Start(b)
                return Meta.Utf8, Meta.utf8End(b), nothing
            else # if O == Int64
                Meta.largUtf8Start(b)
                return Meta.LargeUtf8, Meta.largUtf8End(b), nothing
            end
        else # if Vector{UInt8}
            if O == Int32
                Meta.binaryStart(b)
                return Meta.Binary, Meta.binaryEnd(b), nothing
            else # if O == Int64
                Meta.largeBinaryStart(b)
                return Meta.LargeBinary, Meta.largeBinaryEnd(b), nothing
            end
        end
    else
        children = [fieldoffset(b, "", x.data)]
        if O == Int32
            Meta.listStart(b)
            return Meta.List, Meta.listEnd(b), children
        else
            Meta.largeListStart(b)
            return Meta.LargeList, Meta.largeListEnd(b), children
        end
    end
end

function juliaeltype(f::Meta.Field, list::Meta.FixedSizeList, convert)
    type = juliaeltype(f.children[1], buildmetadata(f.children[1]), convert)
    return NTuple{Int(list.listSize), type}
end

function arrowtype(b, x::FixedSizeList{T, A}) where {T, A}
    N = ArrowTypes.getsize(Base.nonmissingtype(T))
    if eltype(A) == UInt8
        Meta.fixedSizeBinaryStart(b)
        Meta.fixedSizeBinaryAddByteWidth(b, Int32(N))
        return Meta.FixedSizeBinary, Meta.fixedSizeBinaryEnd(b), nothing
    else
        children = [fieldoffset(b, "", x.data)]
        Meta.fixedSizeListStart(b)
        Meta.fixedSizeListAddListSize(b, Int32(N))
        return Meta.FixedSizeList, Meta.fixedSizeListEnd(b), children
    end
end

function juliaeltype(f::Meta.Field, map::Meta.Map, convert)
    K = juliaeltype(f.children[1].children[1], buildmetadata(f.children[1].children[1]), convert)
    V = juliaeltype(f.children[1].children[2], buildmetadata(f.children[1].children[2]), convert)
    return Dict{K, V}
end

function arrowtype(b, x::Map)
    children = [fieldoffset(b, "entries", x.data)]
    Meta.mapStart(b)
    return Meta.Map, Meta.mapEnd(b), children
end

struct KeyValue{K, V}
    key::K
    value::V
end
keyvalueK(::Type{KeyValue{K, V}}) where {K, V} = K
keyvalueV(::Type{KeyValue{K, V}}) where {K, V} = V
Base.length(kv::KeyValue) = 1
Base.iterate(kv::KeyValue, st=1) = st === nothing ? nothing : (kv, nothing)
ArrowTypes.default(::Type{KeyValue{K, V}}) where {K, V} = KeyValue(default(K), default(V))

function arrowtype(b, ::Type{KeyValue{K, V}}) where {K, V}
    children = [fieldoffset(b, "key", K), fieldoffset(b, "value", V)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

function juliaeltype(f::Meta.Field, list::Meta.Struct, convert)
    names = Tuple(Symbol(x.name) for x in f.children)
    types = Tuple(juliaeltype(x, buildmetadata(x), convert) for x in f.children)
    return NamedTuple{names, Tuple{types...}}
end

function arrowtype(b, x::Struct{T, S}) where {T, S}
    names = fieldnames(Base.nonmissingtype(T))
    children = [fieldoffset(b, names[i], x.data[i]) for i = 1:length(names)]
    Meta.structStart(b)
    return Meta.Struct, Meta.structEnd(b), children
end

# Unions
function juliaeltype(f::Meta.Field, u::Meta.Union, convert)
    return Union{(juliaeltype(x, buildmetadata(x), convert) for x in f.children)...}
end

arrowtype(b, x::Union{DenseUnion{TT, S}, SparseUnion{TT, S}}) where {TT, S} = arrowtype(b, TT, x)
function arrowtype(b, ::Type{UnionT{T, typeIds, U}}, x::Union{DenseUnion{TT, S}, SparseUnion{TT, S}}) where {T, typeIds, U, TT, S}
    if typeIds !== nothing
        Meta.unionStartTypeIdsVector(b, length(typeIds))
        for id in Iterators.reverse(typeIds)
            FlatBuffers.prepend!(b, id)
        end
        TI = FlatBuffers.endvector!(b, length(typeIds))
    end
    children = [fieldoffset(b, "", x.data[i]) for i = 1:fieldcount(U)]
    Meta.unionStart(b)
    Meta.unionAddMode(b, T)
    if typeIds !== nothing
        Meta.unionAddTypeIds(b, TI)
    end
    return Meta.Union, Meta.unionEnd(b), children
end

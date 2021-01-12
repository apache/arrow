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

module ArrowJSON

using Mmap
using StructTypes, JSON3, Tables, SentinelArrays, Arrow

# read json files as "table"
# write to arrow stream/file
# read arrow stream/file back

abstract type Type end
Type() = Null("null")
StructTypes.StructType(::Base.Type{Type}) = StructTypes.AbstractType()

children(::Base.Type{T}) where {T} = Field[]

mutable struct Int <: Type
    name::String
    bitWidth::Int64
    isSigned::Base.Bool
end

Int() = Int("", 0, true)
Type(::Base.Type{T}) where {T <: Integer} = Int("int", 8 * sizeof(T), T <: Signed)
StructTypes.StructType(::Base.Type{Int}) = StructTypes.Mutable()
function juliatype(f, x::Int)
    T = x.bitWidth == 8 ? Int8 : x.bitWidth == 16 ? Int16 :
        x.bitWidth == 32 ? Int32 : x.bitWidth == 64 ? Int64 : Int128
    return x.isSigned ? T : unsigned(T)
end

struct FloatingPoint <: Type
    name::String
    precision::String
end

Type(::Base.Type{T}) where {T <: AbstractFloat} = FloatingPoint("floatingpoint", T == Float16 ? "HALF" : T == Float32 ? "SINGLE" : "DOUBLE")
StructTypes.StructType(::Base.Type{FloatingPoint}) = StructTypes.Struct()
juliatype(f, x::FloatingPoint) = x.precision == "HALF" ? Float16 : x.precision == "SINGLE" ? Float32 : Float64

struct FixedSizeBinary <: Type
    name::String
    byteWidth::Int64
end

Type(::Base.Type{NTuple{N, UInt8}}) where {N} = FixedSizeBinary("fixedsizebinary", N)
children(::Base.Type{NTuple{N, UInt8}}) where {N} = Field[]
StructTypes.StructType(::Base.Type{FixedSizeBinary}) = StructTypes.Struct()
juliatype(f, x::FixedSizeBinary) = NTuple{x.byteWidth, UInt8}

struct Decimal <: Type
    name::String
    precision::Int32
    scale::Int32
end

Type(::Base.Type{Arrow.Decimal{P, S, T}}) where {P, S, T} = Decimal("decimal", P, S)
StructTypes.StructType(::Base.Type{Decimal}) = StructTypes.Struct()
juliatype(f, x::Decimal) = Arrow.Decimal{x.precision, x.scale, Int128}

mutable struct Timestamp <: Type
    name::String
    unit::String
    timezone::Union{Nothing ,String}
end

Timestamp() = Timestamp("", "", nothing)
unit(U) = U == Arrow.Meta.TimeUnit.SECOND ? "SECOND" :
          U == Arrow.Meta.TimeUnit.MILLISECOND ? "MILLISECOND" :
          U == Arrow.Meta.TimeUnit.MICROSECOND ? "MICROSECOND" : "NANOSECOND"
Type(::Base.Type{Arrow.Timestamp{U, TZ}}) where {U, TZ} = Timestamp("timestamp", unit(U), TZ === nothing ? nothing : String(TZ))
StructTypes.StructType(::Base.Type{Timestamp}) = StructTypes.Mutable()
unitT(u) = u == "SECOND" ? Arrow.Meta.TimeUnit.SECOND :
           u == "MILLISECOND" ? Arrow.Meta.TimeUnit.MILLISECOND :
           u == "MICROSECOND" ? Arrow.Meta.TimeUnit.MICROSECOND : Arrow.Meta.TimeUnit.NANOSECOND
juliatype(f, x::Timestamp) = Arrow.Timestamp{unitT(x.unit), x.timezone === nothing ? nothing : Symbol(x.timezone)}

struct Duration <: Type
    name::String
    unit::String
end

Type(::Base.Type{Arrow.Duration{U}}) where {U} = Duration("duration", unit(U))
StructTypes.StructType(::Base.Type{Duration}) = StructTypes.Struct()
juliatype(f, x::Duration) = Arrow.Duration{unit%(x.unit)}

struct Date <: Type
    name::String
    unit::String
end

Type(::Base.Type{Arrow.Date{U, T}}) where {U, T} = Date("date", U == Arrow.Meta.DateUnit.DAY ? "DAY" : "MILLISECOND")
StructTypes.StructType(::Base.Type{Date}) = StructTypes.Struct()
juliatype(f, x::Date) = Arrow.Date{x.unit == "DAY" ? Arrow.Meta.DateUnit.DAY : Arrow.Meta.DateUnit.MILLISECOND, x.unit == "DAY" ? Int32 : Int64}

struct Time <: Type
    name::String
    unit::String
    bitWidth::Int64
end

Type(::Base.Type{Arrow.Time{U, T}}) where {U, T} = Time("time", unit(U), 8 * sizeof(T))
StructTypes.StructType(::Base.Type{Time}) = StructTypes.Struct()
juliatype(f, x::Time) = Arrow.Time{unitT(x.unit), x.unit == "SECOND" || x.unit == "MILLISECOND" ? Int32 : Int64}

struct Interval <: Type
    name::String
    unit::String
end

Type(::Base.Type{Arrow.Interval{U, T}}) where {U, T} = Interval("interval", U == Arrow.Meta.IntervalUnit.YEAR_MONTH ? "YEAR_MONTH" : "DAY_TIME")
StructTypes.StructType(::Base.Type{Interval}) = StructTypes.Struct()
juliatype(f, x::Interval) = Arrow.Interval{x.unit == "YEAR_MONTH" ? Arrow.Meta.IntervalUnit.YEAR_MONTH : Arrow.Meta.IntervalUnit.DAY_TIME, x.unit == "YEAR_MONTH" ? Int32 : Int64}

struct UnionT <: Type
    name::String
    mode::String
    typIds::Vector{Int64}
end

Type(::Base.Type{Arrow.UnionT{T, typeIds, U}}) where {T, typeIds, U} = UnionT("union", T == Arrow.Meta.UnionMode.Dense ? "DENSE" : "SPARSE", collect(typeIds))
children(::Base.Type{Arrow.UnionT{T, typeIds, U}}) where {T, typeIds, U} = Field[Field("", fieldtype(U, i), nothing) for i = 1:fieldcount(U)]
StructTypes.StructType(::Base.Type{UnionT}) = StructTypes.Struct()
juliatype(f, x::UnionT) = Arrow.UnionT{x.mode == "DENSE" ? Arrow.Meta.UnionMode.DENSE : Arrow.Meta.UnionMode.SPARSE, Tuple(x.typeIds), Tuple{(juliatype(y) for y in f.children)...}}

struct List <: Type
    name::String
end

Type(::Base.Type{Vector{T}}) where {T} = List("list")
children(::Base.Type{Vector{T}}) where {T} = [Field("item", T, nothing)]
StructTypes.StructType(::Base.Type{List}) = StructTypes.Struct()
juliatype(f, x::List) = Vector{juliatype(f.children[1])}

struct LargeList <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeList}) = StructTypes.Struct()
juliatype(f, x::LargeList) = Vector{juliatype(f.children[1])}

struct FixedSizeList <: Type
    name::String
    listSize::Int64
end

Type(::Base.Type{NTuple{N, T}}) where {N, T} = FixedSizeList("fixedsizelist", N)
children(::Base.Type{NTuple{N, T}}) where {N, T} = [Field("item", T, nothing)]
StructTypes.StructType(::Base.Type{FixedSizeList}) = StructTypes.Struct()
juliatype(f, x::FixedSizeList) = NTuple{x.listSize, juliatype(f.children[1])}

struct Struct <: Type
    name::String
end

Type(::Base.Type{NamedTuple{names, types}}) where {names, types} = Struct("struct")
children(::Base.Type{NamedTuple{names, types}}) where {names, types} = [Field(names[i], fieldtype(types, i), nothing) for i = 1:length(names)]
StructTypes.StructType(::Base.Type{Struct}) = StructTypes.Struct()
juliatype(f, x::Struct) = NamedTuple{Tuple(Symbol(x.name) for x in f.children), Tuple{(juliatype(y) for y in f.children)...}}

struct Map <: Type
    name::String
    keysSorted::Base.Bool
end

Type(::Base.Type{Dict{K, V}}) where {K, V} = Map("map", false)
children(::Base.Type{Dict{K, V}}) where {K, V} = [Field("entries", Arrow.KeyValue{K, V}, nothing)]
StructTypes.StructType(::Base.Type{Map}) = StructTypes.Struct()
juliatype(f, x::Map) = Dict{juliatype(f.children[1].children[1]), juliatype(f.children[1].children[2])}

Type(::Base.Type{Arrow.KeyValue{K, V}}) where {K, V} = Struct("struct")
children(::Base.Type{Arrow.KeyValue{K, V}}) where {K, V} = [Field("key", K, nothing), Field("value", V, nothing)]

struct Null <: Type
    name::String
end

Type(::Base.Type{Missing}) = Null("null")
StructTypes.StructType(::Base.Type{Null}) = StructTypes.Struct()
juliatype(f, x::Null) = Missing

struct Utf8 <: Type
    name::String
end

Type(::Base.Type{<:String}) = Utf8("utf8")
StructTypes.StructType(::Base.Type{Utf8}) = StructTypes.Struct()
juliatype(f, x::Utf8) = String

struct LargeUtf8 <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeUtf8}) = StructTypes.Struct()
juliatype(f, x::LargeUtf8) = String

struct Binary <: Type
    name::String
end

Type(::Base.Type{Vector{UInt8}}) = Binary("binary")
children(::Base.Type{Vector{UInt8}}) = Field[]
StructTypes.StructType(::Base.Type{Binary}) = StructTypes.Struct()
juliatype(f, x::Binary) = Vector{UInt8}

struct LargeBinary <: Type
    name::String
end

StructTypes.StructType(::Base.Type{LargeBinary}) = StructTypes.Struct()
juliatype(f, x::LargeBinary) = Vector{UInt8}

struct Bool <: Type
    name::String
end

Type(::Base.Type{Base.Bool}) = Bool("bool")
StructTypes.StructType(::Base.Type{Bool}) = StructTypes.Struct()
juliatype(f, x::Bool) = Base.Bool

StructTypes.subtypekey(::Base.Type{Type}) = :name

const SUBTYPES = @eval (
    int=Int,
    floatingpoint=FloatingPoint,
    fixedsizebinary=FixedSizeBinary,
    decimal=Decimal,
    timestamp=Timestamp,
    duration=Duration,
    date=Date,
    time=Time,
    interval=Interval,
    union=UnionT,
    list=List,
    largelist=LargeList,
    fixedsizelist=FixedSizeList,
    $(Symbol("struct"))=Struct,
    map=Map,
    null=Null,
    utf8=Utf8,
    largeutf8=LargeUtf8,
    binary=Binary,
    largebinary=LargeBinary,
    bool=Bool
)

StructTypes.subtypes(::Base.Type{Type}) = SUBTYPES

const Metadata = Union{Nothing, Vector{NamedTuple{(:key, :value), Tuple{String, String}}}}
Metadata() = nothing

mutable struct DictEncoding
    id::Int64
    indexType::Type
    isOrdered::Base.Bool
end

DictEncoding() = DictEncoding(0, Type(), false)
StructTypes.StructType(::Base.Type{DictEncoding}) = StructTypes.Mutable()

mutable struct Field
    name::String
    nullable::Base.Bool
    type::Type
    children::Vector{Field}
    dictionary::Union{DictEncoding, Nothing}
    metadata::Metadata
end

Field() = Field("", true, Type(), Field[], nothing, Metadata())
StructTypes.StructType(::Base.Type{Field}) = StructTypes.Mutable()
Base.copy(f::Field) = Field(f.name, f.nullable, f.type, f.children, f.dictionary, f.metadata)

function juliatype(f::Field)
    T = juliatype(f, f.type)
    return f.nullable ? Union{T, Missing} : T
end

function Field(nm, ::Base.Type{T}, dictencodings) where {T}
    S = Arrow.maybemissing(T)
    type = Type(S)
    ch = children(S)
    if dictencodings !== nothing && haskey(dictencodings, nm)
        dict = dictencodings[nm]
    else
        dict = nothing
    end
    return Field(nm, T !== S, type, ch, dict, nothing)
end

mutable struct Schema
    fields::Vector{Field}
    metadata::Metadata
end

Schema() = Schema(Field[], Metadata())
StructTypes.StructType(::Base.Type{Schema}) = StructTypes.Mutable()

struct Offsets{T} <: AbstractVector{T}
    data::Vector{T}
end

Base.size(x::Offsets) = size(x.data)
Base.getindex(x::Offsets, i::Base.Int) = getindex(x.data, i)

mutable struct FieldData
    name::String
    count::Int64
    VALIDITY::Union{Nothing, Vector{Int8}}
    OFFSET::Union{Nothing, Offsets}
    TYPE_ID::Union{Nothing, Vector{Int8}}
    DATA::Union{Nothing, Vector{Any}}
    children::Vector{FieldData}
end

FieldData() = FieldData("", 0, nothing, nothing, nothing, nothing, FieldData[])
StructTypes.StructType(::Base.Type{FieldData}) = StructTypes.Mutable()

function FieldData(nm, ::Base.Type{T}, col, dictencodings) where {T}
    if dictencodings !== nothing && haskey(dictencodings, nm)
        refvals = DataAPI.refarray(col.data)
        if refvals !== col.data
            IT = eltype(refvals)
            col = (x - one(T) for x in refvals)
        else
            _, de = dictencodings[nm]
            IT = de.indexType
            vals = unique(col)
            col = Arrow.DictEncoder(col, vals, Arrow.encodingtype(length(vals)))
        end
        return FieldData(nm, IT, col, nothing)
    end
    S = Arrow.maybemissing(T)
    len = Arrow._length(col)
    VALIDITY = OFFSET = TYPE_ID = DATA = nothing
    children = FieldData[]
    if S <: Pair
        return FieldData(nm, Vector{Arrow.KeyValue{Arrow._keytype(S), Arrow._valtype(S)}}, (Arrow.KeyValue(k, v) for (k, v) in pairs(col)))
    elseif S !== Missing
        # VALIDITY
        VALIDITY = Int8[!ismissing(x) for x in col]
        # OFFSET
        if S <: Vector || S == String
            lenfun = S == String ? x->ismissing(x) ? 0 : sizeof(x) : x->ismissing(x) ? 0 : length(x)
            tot = sum(lenfun, col)
            if tot > 2147483647
                OFFSET = String[String(lenfun(x)) for x in col]
                pushfirst!(OFFSET, "0")
            else
                OFFSET = Int32[ismissing(x) ? 0 : lenfun(x) for x in col]
                pushfirst!(OFFSET, 0)
            end
            OFFSET = Offsets(OFFSET)
            push!(children, FieldData("item", eltype(S), Arrow.flatten(skipmissing(col)), dictencodings))
        elseif S <: NTuple
            if Arrow.ArrowTypes.gettype(S) == UInt8
                DATA = [ismissing(x) ? Arrow.ArrowTypes.default(S) : String(collect(x)) for x in col]
            else
                push!(children, FieldData("item", Arrow.ArrowTypes.gettype(S), Arrow.flatten(coalesce(x, Arrow.ArrowTypes.default(S)) for x in col), dictencodings))
            end
        elseif S <: NamedTuple
            for (nm, typ) in zip(fieldnames(S), fieldtypes(S))
                push!(children, FieldData(String(nm), typ, (getfield(x, nm) for x in col), dictencodings))
            end
        elseif S <: Arrow.UnionT
            U = eltype(S)
            tids = Arrow.typeids(S) === nothing ? (0:fieldcount(U)) : Arrow.typeids(S)
            TYPE_ID = [x === missing ? 0 : tids[Arrow.isatypeid(x, U)] for x in col]
            if Arrow.unionmode(S) == Arrow.Meta.UnionMode.Dense
                offs = zeros(Int32, fieldcount(U))
                OFFSET = Int32[]
                for x in col
                    idx = x === missing ? 1 : Arrow.isatypeid(x, U)
                    push!(OFFSET, offs[idx])
                    offs[idx] += 1
                end
                for i = 1:fieldcount(U)
                    SS = fieldtype(U, i)
                    push!(children, FieldData("$i", SS, Arrow.filtered(i == 1 ? Union{SS, Missing} : Arrow.maybemissing(SS), col), dictencodings))
                end
            else
                for i = 1:fieldcount(U)
                    SS = fieldtype(U, i)
                    push!(children, FieldData("$i", SS, Arrow.replaced(SS, col), dictencodings))
                end
            end
        elseif S <: KeyValue
            push!(children, FieldData("key", Arrow.keyvalueK(S), (x.key for x in col), dictencodings))
            push!(children, FieldData("value", Arrow.keyvalueV(S), (x.value for x in col), dictencodings))
        end
    end
    return FieldData(nm, len, VALIDITY, OFFSET, TYPE_ID, DATA, children)
end

mutable struct RecordBatch
    count::Int64
    columns::Vector{FieldData}
end

RecordBatch() = RecordBatch(0, FieldData[])
StructTypes.StructType(::Base.Type{RecordBatch}) = StructTypes.Mutable()

mutable struct DictionaryBatch
    id::Int64
    data::RecordBatch
end

DictionaryBatch() = DictionaryBatch(0, RecordBatch())
StructTypes.StructType(::Base.Type{DictionaryBatch}) = StructTypes.Mutable()

mutable struct DataFile <: Tables.AbstractColumns
    schema::Schema
    batches::Vector{RecordBatch}
    dictionaries::Vector{DictionaryBatch}
end

Base.propertynames(x::DataFile) = (:schema, :batches, :dictionaries)

function Base.getproperty(df::DataFile, nm::Symbol)
    if nm === :schema
        return getfield(df, :schema)
    elseif nm === :batches
        return getfield(df, :batches)
    elseif nm === :dictionaries
        return getfield(df, :dictionaries)
    end
    return Tables.getcolumn(df, nm)
end

DataFile() = DataFile(Schema(), RecordBatch[], DictionaryBatch[])
StructTypes.StructType(::Base.Type{DataFile}) = StructTypes.Mutable()

parsefile(file) = JSON3.read(Mmap.mmap(file), DataFile)

# make DataFile satisfy Tables.jl interface
function Tables.partitions(x::DataFile)
    if isempty(x.batches)
        # special case empty batches by producing a single DataFile w/ schema
        return (DataFile(x.schema, RecordBatch[], x.dictionaries),)
    else
        return (DataFile(x.schema, [x.batches[i]], x.dictionaries) for i = 1:length(x.batches))
    end
end

Tables.columns(x::DataFile) = x

function Tables.schema(x::DataFile)
    names = map(x -> x.name, x.schema.fields)
    types = map(x -> juliatype(x), x.schema.fields)
    return Tables.Schema(names, types)
end

Tables.columnnames(x::DataFile) =  map(x -> Symbol(x.name), x.schema.fields)

function Tables.getcolumn(x::DataFile, i::Base.Int)
    field = x.schema.fields[i]
    type = juliatype(field)
    return ChainedVector(ArrowArray{type}[ArrowArray{type}(field, length(x.batches) > 0 ? x.batches[j].columns[i] : FieldData(), x.dictionaries) for j = 1:length(x.batches)])
end

function Tables.getcolumn(x::DataFile, nm::Symbol)
    i = findfirst(x -> x.name == String(nm), x.schema.fields)
    return Tables.getcolumn(x, i)
end

struct ArrowArray{T} <: AbstractVector{T}
    field::Field
    fielddata::FieldData
    dictionaries::Vector{DictionaryBatch}
end
ArrowArray(f::Field, fd::FieldData, d) = ArrowArray{juliatype(f)}(f, fd, d)
Base.size(x::ArrowArray) = (x.fielddata.count,)

function Base.getindex(x::ArrowArray{T}, i::Base.Int) where {T}
    @boundscheck checkbounds(x, i)
    S = Base.nonmissingtype(T)
    if x.field.dictionary !== nothing
        fielddata = x.dictionaries[findfirst(y -> y.id == x.field.dictionary.id, x.dictionaries)].data.columns[1]
        field = copy(x.field)
        field.dictionary = nothing
        idx = x.fielddata.DATA[i] + 1
        return ArrowArray(field, fielddata, x.dictionaries)[idx]
    end
    if T === Missing
        return missing
    elseif S <: UnionT
        U = eltype(S)
        tids = Arrow.typeids(S) === nothing ? (0:fieldcount(U)) : Arrow.typeids(S)
        typeid = tids[x.fielddata.TYPE_ID[i]]
        if Arrow.unionmode(S) == Arrow.Meta.UnionMode.DENSE
            off = x.fielddata.OFFSET[i]
            return ArrowArray(x.field.children[typeid+1], x.fielddata.children[typeid+1], x.dictionaries)[off]
        else
            return ArrowArray(x.field.children[typeid+1], x.fielddata.children[typeid+1], x.dictionaries)[i]
        end
    end
    x.fielddata.VALIDITY[i] == 0 && return missing
    if S <: Vector{UInt8}
        return copy(codeunits(x.fielddata.DATA[i]))
    elseif S <: String
        return x.fielddata.DATA[i]
    elseif S <: Vector
        offs = x.fielddata.OFFSET
        A = ArrowArray{eltype(S)}(x.field.children[1], x.fielddata.children[1], x.dictionaries)
        return A[(offs[i] + 1):offs[i + 1]]
    elseif S <: Dict
        offs = x.fielddata.OFFSET
        A = ArrowArray(x.field.children[1], x.fielddata.children[1], x.dictionaries)
        return Dict(y.key => y.value for y in A[(offs[i] + 1):offs[i + 1]])
    elseif S <: Tuple
        if Arrow.ArrowTypes.gettype(S) == UInt8
            A = x.fielddata.DATA
            return Tuple(map(UInt8, collect(A[i][1:x.field.type.byteWidth])))
        else
            sz = x.field.type.listSize
            A = ArrowArray{Arrow.ArrowTypes.gettype(S)}(x.field.children[1], x.fielddata.children[1], x.dictionaries)
            off = (i - 1) * sz + 1
            return Tuple(A[off:(off + sz - 1)])
        end
    elseif S <: NamedTuple
        data = (ArrowArray(x.field.children[j], x.fielddata.children[j], x.dictionaries)[i] for j = 1:length(x.field.children))
        return NamedTuple{fieldnames(S)}(Tuple(data))
    elseif S == Int64 || S == UInt64
        return parse(S, x.fielddata.DATA[i])
    elseif S <: Arrow.Decimal
        str = x.fielddata.DATA[i]
        return S(parse(Int128, str))
    elseif S <: Arrow.Date || S <: Arrow.Time
        val = x.fielddata.DATA[i]
        return Arrow.storagetype(S) == Int32 ? S(val) : S(parse(Int64, val))
    elseif S <: Arrow.Timestamp
        return S(parse(Int64, x.fielddata.DATA[i]))
    else
        return S(x.fielddata.DATA[i])
    end
end

# take any Tables.jl source and write out arrow json datafile
function DataFile(source)
    fields = Field[]
    metadata = nothing # TODO?
    batches = RecordBatch[]
    dictionaries = DictionaryBatch[]
    dictencodings = Dict{String, Tuple{Base.Type, DictEncoding}}()
    dictid = Ref(0)
    for (i, tbl1) in Tables.partitions(source)
        tbl = Arrow.toarrowtable(tbl1)
        if i == 1
            sch = Tables.schema(tbl)
            for (nm, T, col) in zip(sch.names, sch.types, Tables.Columns(tbl))
                if col isa Arrow.DictEncode
                    id = dictid[]
                    dictid[] += 1
                    codes = DataAPI.refarray(col.data)
                    if codes !== col.data
                        IT = Type(eltype(codes))
                    else
                        IT = Type(Arrow.encodingtype(length(unique(col))))
                    end
                    dictencodings[String(nm)] = (T, DictEncoding(id, IT, false))
                end
                push!(fields, Field(String(nm), T, dictencodings))
            end
        end
        # build record batch
        len = Tables.rowcount(tbl)
        columns = FieldData[]
        for (nm, T, col) in zip(sch.names, sch.types, Tables.Columns(tbl))
            push!(columns, FieldData(String(nm), T, col, dictencodings))
        end
        push!(batches, RecordBatch(len, columns))
        # build dictionaries
        for (nm, (T, dictencoding)) in dictencodings
            column = FieldData(nm, T, Tables.getcolumn(tbl, nm), nothing)
            recordbatch = RecordBatch(len, [column])
            push!(dictionaries, DictionaryBatch(dictencoding.id, recordbatch))
        end
    end
    schema = Schema(fields, metadata)
    return DataFile(schema, batches, dictionaries)
end

function Base.isequal(df::DataFile, tbl::Arrow.Table)
    Tables.schema(df) == Tables.schema(tbl) || return false
    i = 1
    for (col1, col2) in zip(Tables.Columns(df), Tables.Columns(tbl))
        if !isequal(col1, col2)
            @show i
            return false
        end
        i += 1
    end
    return true
end

end

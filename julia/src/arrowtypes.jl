module ArrowTypes

export ArrowType, NullType, PrimitiveType, ListType, FixedSizeListType, MapType, StructType, UnionType, DictEncodedType

abstract type ArrowType end

ArrowType(x::T) where {T} = ArrowType(T)
ArrowType(::Type{T}) where {T} = isprimitivetype(T) ? PrimitiveType() : StructType()

function arrowconvert end

arrowconvert(T, x) = convert(T, x)
arrowconvert(::Type{Union{T, Missing}}, x) where {T} = arrowconvert(T, x)
arrowconvert(::Type{Union{T, Missing}}, ::Missing) where {T} = missing

struct NullType <: ArrowType end

ArrowType(::Type{Missing}) = NullType()

struct PrimitiveType <: ArrowType end

ArrowType(::Type{<:Integer}) = PrimitiveType()
ArrowType(::Type{<:AbstractFloat}) = PrimitiveType()
ArrowType(::Type{Bool}) = PrimitiveType()

struct ListType <: ArrowType end

isstringtype(T) = false
isstringtype(::Type{Union{T, Missing}}) where {T} = isstringtype(T)

ArrowType(::Type{<:AbstractString}) = ListType()
isstringtype(::Type{<:AbstractString}) = true

ArrowType(::Type{Symbol}) = ListType()
isstringtype(::Type{Symbol}) = true
arrowconvert(::Type{Symbol}, x::String) = Symbol(x)
arrowconvert(::Type{String}, x::Symbol) = String(x)

ArrowType(::Type{<:AbstractArray}) = ListType()

struct FixedSizeListType <: ArrowType end

ArrowType(::Type{NTuple{N, T}}) where {N, T} = FixedSizeListType()
getsize(::Type{NTuple{N, T}}) where {N, T} = N

struct MapType <: ArrowType end

ArrowType(::Type{<:Dict}) = MapType()

struct StructType <: ArrowType end

ArrowType(::Type{<:NamedTuple}) = StructType()

@enum STRUCT_TYPES NAMEDTUPLE STRUCT # KEYWORDARGS

structtype(::Type{NamedTuple{N, T}}) where {N, T} = NAMEDTUPLE
structtype(::Type{T}) where {T} = STRUCT

struct UnionType <: ArrowType end

ArrowType(::Union) = UnionType()

struct DictEncodedType <: ArrowType end

"""
There are a couple places when writing arrow buffers where
we need to write a "dummy" value; it doesn't really matter
what we write, but we need to write something of a specific
type. So each supported writing type needs to define `default`.
"""
function default end

default(T) = zero(T)
default(::Type{Symbol}) = Symbol()
default(::Type{Char}) = '\0'
default(::Type{String}) = ""

function default(::Type{A}) where {A <: AbstractVector{T}} where {T}
    a = similar(A, 1)
    a[1] = default(T)
    return a
end

default(::Type{NTuple{N, T}}) where {N, T} = ntuple(i -> default(T), N)
default(::Type{T}) where {T <: Tuple} = Tuple(default(fieldtype(T, i)) for i = 1:fieldcount(T))
default(::Type{Dict{K, V}}) where {K, V} = Dict{K, V}()
default(::Type{NamedTuple{names, types}}) where {names, types} = NamedTuple{names}(Tuple(default(fieldtype(types, i)) for i = 1:length(names)))

const JULIA_TO_ARROW_TYPE_MAPPING = Dict{Type, Tuple{String, Type}}(
    Char => ("JuliaLang.Char", UInt32),
    Symbol => ("JuliaLang.Symbol", String),
)

istyperegistered(::Type{T}) where {T} = haskey(JULIA_TO_ARROW_TYPE_MAPPING, T)

function getarrowtype!(meta, ::Type{T}) where {T}
    arrowname, arrowtype = JULIA_TO_ARROW_TYPE_MAPPING[T]
    meta["ARROW:extension:name"] = arrowname
    meta["ARROW:extension:metadata"] = ""
    return arrowtype
end

const ARROW_TO_JULIA_TYPE_MAPPING = Dict{String, Tuple{Type, Type}}(
    "JuliaLang.Char" => (Char, UInt32),
    "JuliaLang.Symbol" => (Symbol, String),
)

function extensiontype(meta)
    if haskey(meta, "ARROW:extension:name")
        typename = meta["ARROW:extension:name"]
        if haskey(ARROW_TO_JULIA_TYPE_MAPPING, typename)
            return ARROW_TO_JULIA_TYPE_MAPPING[typename][1]
        else
            @warn "unsupported ARROW:extension:name type: \"$typename\""
        end
    end
    return nothing
end

function registertype!(juliatype::Type, arrowtype::Type, arrowname::String=string("JuliaLang.", string(juliatype)))
    # TODO: validate that juliatype isn't already default arrow type
    JULIA_TO_ARROW_TYPE_MAPPING[juliatype] = (arrowname, arrowtype)
    ARROW_TO_JULIA_TYPE_MAPPING[arrowname] = (juliatype, arrowtype)
    return
end

end # module ArrowTypes

import Base: hcat, vcat


module Locate
    import Arrow
    using Arrow: ALIGNMENT, DefaultOffset

    abstract type Abstract end

    function checkalignment(loc::Integer)
        if (loc-1) % ALIGNMENT ≠ 0
            throw(ArgumentError("Improperly aligned sub-buffer locator value: $loc"))
        end
    end

    containertype(::Type{J}, v) where {J} = Arrow.Primitive
    containertype(::Type{Union{J,Missing}}, v) where {J} = Arrow.NullablePrimitive
    containertype(::Type{String}, v) = Arrow.List
    containertype(::Type{Union{String,Missing}}, v) = Arrow.NullableList
    containertype(::Type{Bool}, v) = Arrow.BitPrimitive
    containertype(::Type{Union{Bool,Missing}}, v) = Arrow.NullableBitPrimitive

    length(v) = Base.length(v)

    function values end
    function valueslength end
    function bitmask end
    function offsets end

    struct Values{J} <: Abstract
        loc::Int64
        len::Int64
        Values{J}(loc::Integer, len::Integer) where {J} = (checkalignment(loc); new{J}(loc, len))
    end
    Values{J}(x) where {J} = Values{J}(values(x), valueslength(x))
    Values(::Type{String}, x) = Values{UInt8}(values(x), valueslength(x))
    Values(::Type{Union{String,Missing}}, x) = Values{UInt8}(values(x), valueslength(x))
    struct Bitmask <: Abstract
        loc::Int64
        Bitmask(loc::Integer) = (checkalignment(loc); new(loc))
    end
    Bitmask(x) = Bitmask(bitmask(x))
    struct Offsets{J<:Integer} <: Abstract
        loc::Int64
        Offsets{J}(loc::Integer) where {J} = (checkalignment(loc); new{J}(loc))
    end
    Offsets{J}(x) where {J} = Offsets{J}(offsets(x))
    Offsets(x) = Offsets{DefaultOffset}(offsets(x))
end

# type constructor methods
function locate(data::Vector{UInt8}, vals::Locate.Values{J}, len::Integer) where J
    Primitive{J}(data, vals.loc, len)
end
function locate(data::Vector{UInt8}, bmask::Locate.Bitmask, vals::Locate.Values{J}, len::Integer) where J
    NullablePrimitive{J}(data, bmask.loc, vals.loc, len)
end
function locate(data::Vector{UInt8}, ::Type{J}, offs::Locate.Offsets{K}, vals::Locate.Values{C},
                len::Integer) where {J,K<:Integer,C}
    List{J,K}(data, offs.loc, vals.loc, len, C, vals.len)
end
function locate(data::Vector{UInt8}, ::Type{Union{J,Missing}}, bmask::Locate.Bitmask,
                offs::Locate.Offsets{K}, vals::Locate.Values{C}, len::Integer) where {J,K<:Integer,C}
    NullableList{J,K}(data, bmask.loc, offs.loc, vals.loc, len, C, vals.len)
end


# method called by users
function locate(data::Vector{UInt8}, ::Type{J}, vec::T) where {J,T}
    locate(Locate.containertype(J, vec), data, J, vec)
end

# these call the locate methods above
function locate(::Type{Primitive}, data::Vector{UInt8}, ::Type{J}, vec::T) where {J,T}
    locate(data, Locate.Values{J}(vec), Locate.length(vec))
end
function locate(::Type{NullablePrimitive}, data::Vector{UInt8}, ::Type{Union{J,Missing}}, vec::T
               ) where {J,T}
    locate(data, Locate.Bitmask(vec), Locate.Values{J}(vec), Locate.length(vec))
end
function locate(::Type{List}, data::Vector{UInt8}, ::Type{J}, vec::T) where {J,T}
    locate(data, J, Locate.Offsets(vec), Locate.Values(J, vec), Locate.length(vec))
end
function locate(::Type{NullableList}, data::Vector{UInt8}, ::Type{J}, vec::T) where {J,T}
    locate(data, J, Locate.Bitmask(vec), Locate.Offsets(vec), Locate.Values(J, vec), Locate.length(vec))
end
# these are a little inflexible and need more specific declarations
function locate(::Type{BitPrimitive}, data::Vector{UInt8}, ::Type{Bool}, vec::T) where {T}
    vals = Locate.Values{Bool}(vec)
    BitPrimitive(data, vals.loc, Locate.length(vec))
end
function locate(::Type{NullableBitPrimitive}, data::Vector{UInt8}, ::Type{Union{Bool,Missing}},
                vec::T) where {T}
    bmask = Locate.Bitmask(vec)
    vals = Locate.Values{Bool}(vec)
    NullableBitPrimitive(data, bmask.loc, vals.loc, Locate.length(vec))
end


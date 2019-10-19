
# TODO bounds checking in constructors for all indices

abstract type AbstractPrimitive{J} <: ArrowVector{J} end


"""
    Primitive{J} <: AbstractPrimitive{J}

A set of values which are stored contiguously in memory.  All other `ArrowVector` objects are built
from `Primitive`s.

## Constructors
    Primitive{J}(data::Vector{UInt8}, i::Integer, len::Integer)
    Primitive(data::Vector{UInt8}, i::Integer, x::AbstractVector)
    Primitive(v::AbstractVector)
    Primitive(Array, x::Abstractvector)

If `Array` is provided as the virst argument, the data will be allocated contiguously in a new buffer.

### Arguments
- `data`: a data buffer which the `Primitive` will refer to for accessing and storing values
- `i`: the index of the location (1-based) in `data` where the beginning of the value data is stored
- `x`: a vector which will be written into `data[i]` on construction
` `v`: existing reference data. constructors with will reference the original `v` as `data`
"""
struct Primitive{J} <: AbstractPrimitive{J}
    length::Int64
    values_idx::Int64
    data::Vector{UInt8}
end

function Primitive{J}(data::AbstractVector{UInt8}, i::Integer, len::Integer) where J
    @boundscheck check_buffer_bounds(J, data, i, len)
    Primitive{J}(Int64(len), Int64(i), data)
end
function Primitive(data::AbstractVector{UInt8}, i::Integer, x::AbstractVector{J}) where J
    p = Primitive{J}(data, i, length(x))
    p[:] = x
    p
end
function Primitive{J}(data::AbstractVector{UInt8}, i::Integer, x::AbstractVector{K}) where {J,K}
    Primitive(data, i, convert(AbstractVector{J}, x))
end

# view of reinterpreted, will not include padding
function Primitive(v::AbstractVector{J}) where J
    b = convert(Vector{UInt8}, reinterpret(UInt8, v))
    Primitive{J}(b, 1, length(v))
end
function Primitive{J}(v::AbstractVector{T}) where {J,T}
    Primitive(convert(AbstractVector{J}, v))
end

# create own buffer
function Primitive{J}(::Type{<:Array}, v::AbstractVector) where J
    b = Vector{UInt8}(undef, totalbytes(v))
    Primitive{J}(b, 1, v)
end
Primitive(::Type{<:Array}, v::AbstractVector) = Primitive(v)

# use undef buffer
function Primitive{J}(::UndefInitializer, i::Integer, len::Integer) where J
    b = Vector{UInt8}(undef, valuesbytes(J, len))
    Primitive{J}(b, i, len)
end

Primitive{J}(p::Primitive{J}) where J = Primitive{J}(p.length, p.values_idx, p.data)
Primitive{J}(p::Primitive{T}) where {J,T} = Primitive{J}(convert(AbstractVector{J}, p[:]))
Primitive(p::Primitive{J}) where J = Primitive{J}(p)


"""
    datapointer(A::Primitive)

Returns a pointer to the very start of the data buffer for `A` (i.e. does not depend on indices).
"""
datapointer(A::Primitive) = pointer(A.data)

valuespointer(A::Primitive) = datapointer(A) + A.values_idx - 1


#==================================================================================================
    NullablePrimitive
==================================================================================================#
"""
    NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}

A set of values stored contiguously in data with a bit mask specifying which values should be considered
null.  The bit mask and data needn't necessarily coexist within the same array.

## Constructors
    NullablePrimitive(bmask::Primitive{UInt8}, vals::Primitive{J})
    NullablePrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer, x::AbstractVector)
    NullablePrimitive(data::Vector{UInt8}, i::Integer, x::AbstractVector)
    NullablePrimitive(v::AbstractVector)
    NullablePrimitive(Array, x::AbstractVector)

If `Array` is passed to a constructor, the bit mask and values for the `NullablePrimitive` will be
contiguously allocated within a single array (bit mask first, then values).

### Arguments
- `bmask`: a `Primitive{UInt8}` containing the null bitmask for the `NullablePrimitive`
- `vals`: a `Primitive` containing the underlying data values for the `NullablePrimitive`
- `data`: a buffer in which the data for the `NullablePrimitive` will be stored
- `bitmask_idx`: the location within `data` where the null bit mask will be stored
- `values_idx`: the location within `data` where the values will be stored
- `x`, `v`: values to be stored in data
"""
struct NullablePrimitive{J} <: AbstractPrimitive{Union{J,Missing}}
    length::Int64
    bitmask::Primitive{UInt8}
    values::Primitive{J}
end

# Primitive constructors
function NullablePrimitive{J}(bmask::Primitive{UInt8}, vals::Primitive{J}) where J
    NullablePrimitive{J}(length(vals), bmask, vals)
end
function NullablePrimitive(bmask::Primitive{UInt8}, vals::Primitive{J}) where J
    NullablePrimitive{J}(bmask, vals)
end

# buffer with location constructors
function NullablePrimitive{J}(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                              len::Integer) where J
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmaskbytes(len))
    vals = Primitive{J}(data, values_idx, len)
    NullablePrimitive{J}(bmask, vals)
end
function NullablePrimitive{J}(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                              x::AbstractVector) where J
    bmask = Primitive{UInt8}(data, bitmask_idx, bitmaskpadded(x))
    vals = Primitive{J}(data, values_idx, length(x))
    setnonmissing!(vals, x)
    NullablePrimitive{J}(bmask, vals)
end
function NullablePrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                           x::AbstractVector{J}) where J
    NullablePrimitive{J}(data, bitmask_idx, values_idx, x)
end
function NullablePrimitive(data::Vector{UInt8}, bitmask_idx::Integer, values_idx::Integer,
                           x::AbstractVector{Union{J,Missing}}) where J
    NullablePrimitive{J}(data, bitmask_idx, values_idx, x)
end
function NullablePrimitive(data::Vector{UInt8}, i::Integer, v::AbstractVector{T}
                          ) where {J,T<:Union{J,Union{J,Missing}}}
    NullablePrimitive(data, i, i+bitmaskbytes(v), v)
end
function NullablePrimitive{J}(data::Vector{UInt8}, i::Integer, v::AbstractVector{T}) where {J,T}
    NullablePrimitive(data, i, convert(AbstractVector{J}, v))
end

# contiguous new buffer constructors
function NullablePrimitive(::Type{<:Array}, v::AbstractVector{Union{J,Missing}}) where J
    b = Vector{UInt8}(undef, totalbytes(v))
    NullablePrimitive(b, 1, v)
end
function NullablePrimitive{J}(::Type{K}, v::AbstractVector{T}) where {J,K<:Array,T}
    NullablePrimitive(K, convert(AbstractVector{Union{J,Missing}}, v))
end
function NullablePrimitive(::Type{K}, v::AbstractVector{J}) where {K<:Array,J}
    NullablePrimitive(K, convert(AbstractVector{Union{J,Missing}}, v))
end

# new buffer constructors
function NullablePrimitive(v::AbstractVector{Union{J,Missing}}) where J
    bmask = Primitive(bitmaskpadded(v))
    vals = Primitive(replace_missing_vals(v))  # using first ensures exists
    NullablePrimitive(bmask, vals)
end
function NullablePrimitive(v::AbstractVector{J}) where J
    NullablePrimitive(convert(AbstractVector{Union{J,Missing}}, v))
end
function NullablePrimitive{J}(v::AbstractVector{K}) where {J,K}
    NullablePrimitive(convert(AbstractVector{Union{J,Missing}}, v))
end


function NullablePrimitive{J}(p::NullablePrimitive{J}) where J
    NullablePrimitive{J}(p.length, p.bitmask, p.values)
end
function NullablePrimitive{J}(p::NullablePrimitive{T}) where {J,T}
    NullablePrimitive{J}(convert(AbstractVector{J}, p[:]))
end
NullablePrimitive(p::NullablePrimitive{J}) where J = NullablePrimitive{J}(p)



#================================================================================================

    common interface

================================================================================================#
"""
    valuesbytes(A::AbstractVector)
    valuesbytes(::Type{C}, A::AbstractVector{<:AbstractString})

Computes the number of bytes needed to store the *values* of `A` (without converting the underlying
binary type). This does not include the number of bytes needed to store metadata such as a null
bitmask or offsets.

To obtain the number of values bytes needed to string data, one must input `C` the character encoding
type the string will be converted to (e.g. `UInt8`).
"""
valuesbytes(::Type{J}, len::Integer) where J = padding(sizeof(J)*len)
valuesbytes(A::AbstractVector{J}) where J = valuesbytes(J, length(A))
valuesbytes(A::AbstractVector{Union{J,Missing}}) where J = valuesbytes(J, length(A))

"""
    bitmaskbytes(A::AbstractVector)
    bitmaskbytes(::Type{Union{J,Missing}}, A::AbstractVector)

Compute the number of bytes needed to store a null bitmask for the data in `A`.  This is 0
unless `J <: Union{K,Missing}`. Note that this does not take into account scheme-dependent padding.
"""
bitmaskbytes(len::Integer) = padding(bytesforbits(len))
bitmaskbytes(A::AbstractVector) = 0
bitmaskbytes(::Type{Union{J,Missing}}, A::AbstractVector{J}) where J = bitmaskbytes(length(A))
function minbitmaskbytes(::Type{Union{J,Missing}}, A::AbstractVector{Union{J,Missing}}) where J
    bitmaskbytes(length(A))
end
bitmaskbytes(A::AbstractVector{Union{J,Missing}}) where J = bitmaskbytes(length(A))

minbitmaskbytes(len::Integer) = bytesforbits(len)
minbitmaskbytes(A::AbstractVector) = minbitmaskbytes(length(A))

"""
    totalbytes(A::AbstractVector)
    totalbytes(::Type{Union{J,Missing}}, A::AbstractVector)
    totalbytes(::Type{C}, A::AbstractVector)
    totalbytes(::Type{Union{J,Missing}}, ::Type{C}, A::AbstractVector)

Computes the minimum number of bytes needed to store `A` as an Arrow formatted primitive array or list.

To obtain the minimum bytes to store string data, one must input `C` the character encoding type the
string will be converted to (e.g. `UInt8`).
"""
totalbytes(A::AbstractVector) = bitmaskbytes(A) + valuesbytes(A)
function totalbytes(::Type{Union{J,Missing}}, A::AbstractVector{J}) where J
    bitmaskbytes(Union{J,Missing}, A) + valuesbytes(A)
end
function totalbytes(::Type{Union{J,Missing}}, A::AbstractVector{Union{J,Missing}}) where J
    bitmaskbytes(Union{J,Missing}, A) + valuesbytes(A)
end


@inline function unsafe_contiguous(A::Union{Primitive{J},NullablePrimitive{J}},
                                   idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, valuespointer(A)) + (first(idx)-1)*sizeof(J)
    unsafe_wrap(Array, ptr, length(idx))
end


"""
    unsafe_getvalue(A::ArrowVector, i)

Retrieve the value from memory location `i` using Julia 1-based indexing. `i` can be a single integer
index, an `AbstractVector` of integer indices, or an `AbstractVector{Bool}` mask.

This typically involves a call to `unsafe_load` or `unsafe_wrap`.
"""
function unsafe_getvalue(A::Union{Primitive{J},NullablePrimitive{J}},
                         i::Integer)::J where J
    unsafe_load(convert(Ptr{J}, valuespointer(A)), i)
end
function unsafe_getvalue(A::AbstractPrimitive{T}, idx::UnitRange{<:Integer}) where T
    copyto!(Vector{T}(undef, length(idx)), unsafe_contiguous(A, idx))
end
function unsafe_getvalue(A::AbstractPrimitive{T}, idx::AbstractVector{<:Integer}) where T
    T[unsafe_getvalue(A, i) for i ∈ idx]
end
function unsafe_getvalue(A::AbstractPrimitive{T}, idx::AbstractVector{Bool}) where T
    T[unsafe_getvalue(A, i) for i ∈ 1:length(A) if idx[i]]
end
unsafe_getvalue(A::Primitive, ::Colon) = unsafe_getvalue(A, 1:length(A))


_rawvalueindex_start(A::Primitive{J}, i::Integer) where J = A.values_idx + (i-1)*sizeof(J)
_rawvalueindex_stop(A::Primitive{J}, i::Integer) where J = A.values_idx + i*sizeof(J) - 1

function rawvalueindex_contiguous(A::Primitive{J}, idx::AbstractVector{<:Integer}) where J
    a = _rawvalueindex_start(A, first(idx))
    b = _rawvalueindex_stop(A, last(idx))
    a:b
end


function rawvalueindex(A::Primitive{J}, i::Integer) where J
    a = _rawvalueindex_start(A, i)
    b = _rawvalueindex_stop(A, i)
    a:b
end
function rawvalueindex(A::Primitive, idx::AbstractVector{<:Integer})
    vcat((rawvalueindex(A, i) for i ∈ idx)...)  # TODO inefficient use of vcat
end
rawvalueindex(A::Primitive, idx::UnitRange{<:Integer}) = rawvalueindex_contiguous(A, idx)
function rawvalueindex(A::Primitive, idx::AbstractVector{Bool})
    rawvalueindex(A, [i for i ∈ 1:length(A) if idx[i]])
end


# TODO should these return views?
"""
    rawvalues(A::Primitive, i)
    rawvalues(A::Primitive)

Gets the raw values for elements at locations `i` in the form of a `Vector{UInt8}`.
"""
rawvalues(A::Primitive, i::Union{<:Integer,AbstractVector{<:Integer}}) = A.data[rawvalueindex(A, i)]
rawvalues(A::Primitive, ::Colon) = rawvalues(A, 1:length(A))
rawvalues(A::Primitive) = rawvalues(A, :)


"""
    getvalue(A::ArrowVector, idx)

Get the values for indices `idx` from `A`.
"""
function getvalue(A::Union{Primitive{J},NullablePrimitive{J}}, i::Integer) where J
    reinterpret(J, rawvalues(A, i))[1]
end
function getvalue(A::Primitive{J}, i::AbstractVector{<:Integer}) where J
    copyto!(Vector{J}(undef, length(i)), reinterpret(J, rawvalues(A, i)))
end
function getvalue(A::NullablePrimitive{J}, i::AbstractVector{<:Integer}) where J
    copyto!(Vector{Union{J,Missing}}(undef, length(i)), reinterpret(J, rawvalues(A, i)))
end


"""
    unsafe_rawpaddedvalues(p::ArrowVector)

Retreive raw value data for `p` as a `Vector{UInt8}`.
"""
function unsafe_rawpaddedvalues(p::AbstractPrimitive)
    unsafe_rawpadded(valuespointer(p), valuesbytes(p))
end


function setvalue!(A::Primitive{J}, x::J, i::Integer) where J
    A.data[rawvalueindex(A, i)] = reinterpret(UInt8, [x])
end
function setvalue!(A::Primitive{J}, x::Vector{J}, idx::AbstractVector{<:Integer}) where J
    A.data[rawvalueindex(A, idx)] = reinterpret(UInt8, x)
end
function setvalue!(A::NullablePrimitive{J}, x::J, i::Integer) where J
    setvalue!(A.values, x, i)
end
function setvalue!(A::NullablePrimitive{J}, x::Vector{J}, idx::AbstractVector{<:Integer}) where J
    setvalue!(A.values, x, idx)
end


"""
    unsafe_setvalue!(A::ArrowVector{J}, x, i)

Set the value at location `i` to `x`.  If `i` is a single integer, `x` should be an element of type
`J`.  Otherwise `i` can be an `AbstractVector{<:Integer}` or `AbstractVector{Bool}` in which case
`x` should be an appropriately sized `AbstractVector{J}`.
"""
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, x::J, i::Integer) where J
    unsafe_store!(convert(Ptr{J}, valuespointer(A)), x, i)
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::AbstractVector{J},
                          idx::AbstractVector{<:Integer}) where J
    ptr = convert(Ptr{J}, valuespointer(A))
    for (x, i) ∈ zip(v, idx)
        unsafe_store!(ptr, x, i)
    end
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::AbstractVector{J},
                          idx::AbstractVector{Bool}) where J
    ptr = convert(Ptr{J}, valuespointer(A))
    j = 1
    for i ∈ 1:length(A)
        if idx[i]
            unsafe_store!(ptr, v[j], i)
            j += 1
        end
    end
end
function unsafe_setvalue!(A::Union{Primitive{J},NullablePrimitive{J}}, v::Vector{J}, ::Colon) where J
    unsafe_copy!(convert(Ptr{J}, valuespointer(A)), pointer(v), length(v))
end


# note that this use of unsafe_string is ok because it copies
"""
    unsafe_construct(::Type{T}, A::Primitive, i::Integer, len::Integer)

Construct an object of type `T` using `len` elements from `A` starting at index `i` (1-based indexing).
This is mostly used by `AbstractList` objects to construct variable length objects such as strings
from primitive arrays.

Users must define new methods for new types `T`.
"""
function unsafe_construct(::Type{String}, A::Primitive{UInt8}, i::Integer, len::Integer)
    unsafe_string(convert(Ptr{UInt8}, valuespointer(A) + (i-1)), len)
end
# this is a fallback for other types and is safe
function unsafe_construct(::Type{T}, A::AbstractPrimitive, i::Integer, len::Integer) where T
    construct(T, A, i, len)
end
function unsafe_construct(::Type{T}, A::NullablePrimitive, i::Integer, len::Integer) where T
    throw(ErrorException("Unsafely constructing objects from NullablePrimitves is not supported."))
end


"""
    construct(::Type{T}, A::AbstractPrimitive{J}, i::Integer, len::Integer)

Construct an object of type `T` from `len` values in `A` starting at index `i`.
For this to work requires the existence of a constructor of the form `T(::Vector{J})`.
"""
function construct(::Type{T}, A::AbstractPrimitive, i::Integer, len::Integer) where T
    T(A[i:(i+len-1)])  # obviously this depends on the existence of this constructor
end


function arrowview_contiguous(p::Primitive{J}, idx::AbstractVector{<:Integer}) where J
    Primitive{J}(p.data, _rawvalueindex_start(p, first(idx)), length(idx))
end

"""
    arrowview(p::ArrowVector, idx::UnitRange)

Return another `ArrowVector` which is a view into `p` for the index range `idx`.

This can only be done with contiguous indices.
"""
arrowview(p::Primitive, idx::UnitRange) = arrowview_contiguous(p, idx)


function Base.setindex!(A::Primitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    setvalue!(A, convert(J, x), i)  # should this conversion really be here?
end
# TODO inefficient in some cases because of conversion to Vector{J}
function Base.setindex!(A::Primitive{J}, x::AbstractVector, idx::AbstractVector{<:Integer}) where J
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    setvalue!(A, convert(Vector{J}, x), idx)
end
Base.setindex!(A::Primitive, x::AbstractVector, ::Colon) = (A[1:end] = x)

function Base.setindex!(A::NullablePrimitive{J}, x, i::Integer) where J
    @boundscheck checkbounds(A, i)
    o = setvalue!(A, convert(J, x), i)
    setnull!(A, false, i)  # important that this is last in case above fails
    o
end
function Base.setindex!(A::NullablePrimitive{J}, x::Missing, i::Integer) where J
    @boundscheck checkbounds(A, i)
    setnull!(A, true, i)
    missing
end
# TODO this is horribly inefficient but really hard to do right for non-consecutive
function Base.setindex!(A::NullablePrimitive, x::AbstractVector, idx::AbstractVector{<:Integer})
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    for (ξ,i) ∈ zip(x, idx)
        @inbounds setindex!(A, ξ, i)
    end
end
function Base.setindex!(A::NullablePrimitive, x::AbstractVector, idx::AbstractVector{Bool})
    @boundscheck (checkbounds(A, idx); checkinputsize(x, idx))
    j = 1
    for i ∈ 1:length(A)
        if idx[i]
            @inbounds setindex!(A, x[j], i)
            j += 1
        end
    end
    x
end
# TODO this probably isn't really much more efficient, should test
function Base.setindex!(A::NullablePrimitive{J}, x::AbstractVector, ::Colon) where J
    @boundscheck checkinputsize(x, A)
    setnulls!(A, ismissing.(x))
    for i ∈ 1:length(A)
        !ismissing(x[i]) && setvalue!(A, convert(J, x[i]), i)
    end
    x
end

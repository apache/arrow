
"""
    padding(n::Integer)

Determines the total number of bytes needed to store `n` bytes with padding.
Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
"""
padding(n::Integer) = ((n + ALIGNMENT - 1) ÷ ALIGNMENT)*ALIGNMENT


paddinglength(n::Integer) = padding(n) - n


"""
    writepadded(io::IO, x)
    writepadded(io::IO, A::Primitive)
    writepadded(io::IO, A::Arrowvector, subbuffs::Function...)

Write the data `x` to `io` with 8-byte padding. This is commonly needed in Arrow implementations
since Arrow requires 8-byte boundary alignment.

If a `Primitive` is provided, the appropriate padded values will be written.

If an `ArrowVector` is provided, the ordering of the sub-buffers must be specified, and they will
be written in the order given.  For example `writepadded(io, A, bitmask, offsets, values)` will write
the bit mask, offsets and then values of `A`.
"""
function writepadded(io::IO, x)
    bw = write(io, x)
    diff = padding(bw) - bw
    write(io, zeros(UInt8, diff))
    bw + diff
end


"""
    bytesforbits(n::Integer)

Get the number of bytes required to store `n` bits.
"""
bytesforbits(n::Integer) = div(((n + 7) & ~7), 8)

getbit(byte::UInt8, i::Integer) = (byte & BITMASK[i] > 0x00)
function setbit(byte::UInt8, x::Bool, i::Integer)
    if x
        byte | BITMASK[i]
    else
        byte & (~BITMASK[i])
    end
end

# helper function for encode
_encodesingle(::Type{C}, x) where {C} = convert(Vector{C}, x)
_encodesingle(::Type{C}, x::AbstractString) where {C} = convert(Vector{C}, codeunits(x))

# TODO: make this more general so it can be used everywhere
"""
    encode(::Type{C}, v::AbstractVector{J})

Attempt to encode the data in `v` as a `Vector{C}`. This requires that if `convert(Vector{C}, x)` is
valid where `x ∈ v`.  Nothing is stored for cases where `x == missing`.
"""
function encode(::Type{C}, v::AbstractVector{J}) where {C,J}
    mapreduce(x -> _encodesingle(C, x), vcat, v)
end
function encode(::Type{C}, v::AbstractVector{Union{J,Missing}}) where {C,J}
    mapreduce(vcat, v) do x
        ismissing(x) ? Vector{C}(undef, 0) : _encodesingle(C, x)
    end
end

function encode(::Type{C}, v::AbstractVector{J}) where {C, J <: AbstractString}
    N   = mapreduce(length ∘ codeunits, +, v)
    enc = Vector{C}(undef, N)
    i   = 0
    for val in v, c in codeunits(val)
       i += 1
       enc[i] = convert(C, c)
    end
    return enc
end

function encode(::Type{C}, v::AbstractVector{Union{J, Missing}}) where {C, J <: AbstractString}
    N   = mapreduce(x -> ismissing(x) ? 0 : length(codeunits(x)) , +, v)
    enc = Vector{C}(undef, N)
    i   = 0
    for val in v
        if !ismissing(val)
            for c in codeunits(val)
                i += 1
                enc[i] = convert(C, c)
            end
        end
    end
    return enc
end


function replace_missing_vals(A::AbstractVector{Union{J,Missing}}) where J<:Number
    J[ismissing(x) ? zero(J) : x for x ∈ A]
end
function replace_missing_vals(A::AbstractVector{Union{J,Missing}}) where J
    J[ismissing(x) ? first(skipmissing(A)) : x for x ∈ A]  # using first ensures existence
end


# nbits must be ≤ 8
function _bitpack_byte(a::AbstractVector{Bool}, nbits::Integer)
    o = 0x00
    for i ∈ 1:nbits
        o += UInt8(a[i]) << (i-1)
    end
    o
end

"""
    bitpack(A::AbstractVector{Bool})

Returns a `Vector{UInt8}` the bits of which are the values of `A`.
"""
function bitpack(A::AbstractVector{Bool})
    a, b = divrem(length(A), 8)
    trailing = b > 0
    nbytes = a + Int(trailing)
    v = Vector{UInt8}(undef, nbytes)
    for i ∈ 1:a
        k = (i-1)*8 + 1
        v[i] = _bitpack_byte(view(A, k:(k+7)), 8)
    end
    if trailing
        trail = (a*8+1):length(A)
        v[end] = _bitpack_byte(view(A, trail), length(trail))
    end
    v
end
bitpack(A::AbstractVector{Union{Bool,Missing}}) = bitpack(replace_missing_vals(A))


function bitpackpadded(A::AbstractVector{Bool})
    v = bitpack(A)
    npad = paddinglength(length(v))
    vcat(v, zeros(UInt8, npad))
end


function bitmaskpadded(A::AbstractVector)
    v = .!ismissing.(A)
    bitpackpadded(v)
end


"""
    unbitpack(A::AbstractVector{UInt8})

Returns a `Vector{Bool}` the values of which are the bits of `A`.
"""
function unbitpack(A::AbstractVector{UInt8})
    v = Vector{Bool}(length(A)*8)
    for i ∈ 1:length(A)
        for j ∈ 1:8
            v[(i-1)*8 + j] = getbit(A[i], j)
        end
    end
    v
end


function checkinputsize(v::AbstractVector, idx::AbstractVector{<:Integer})
    if length(v) ≠ length(idx)
        throw(DimensionMismatch("tried to assign $(length(v)) elements to $(length(idx)) destinations"))
    end
end
function checkinputsize(v::AbstractVector, idx::AbstractVector{Bool})
    if length(v) ≠ sum(idx)
        throw(DimensionMismatch("tried to assign $(length(v)) elements to $(sum(idx)) destinations"))
    end
end
function checkinputsize(v::AbstractVector, A::ArrowVector)
    if length(v) ≠ length(A)
        throw(DimensionMismatch("tried to assign $(length(v)) elements to $(length(A)) destinations"))
    end
end


# this is only for values buffers
function check_buffer_bounds(::Type{U}, A::AbstractVector, i::Integer, len::Integer) where U
    len == 0 && (return nothing)  # if the array being created is empty these bounds don't matter
    checkbounds(A, i)
    checkbounds(A, i+len*sizeof(U)-1)
end


"""
    unsafe_rawpadded(ptr::Ptr, len::Integer, padding::Function=identity)

Return a `Vector{UInt8}` padded to appropriate size specified by `padding`.
"""
function unsafe_rawpadded(ptr::Ptr{UInt8}, len::Integer)
    npad = padding(len) - len
    vcat(unsafe_wrap(Array, ptr, len), zeros(UInt8, npad))
end



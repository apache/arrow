

const MAX_RAND_PAD_LENGTH_MOD8 = 3


randpad() = rand(UInt8, rand(8*(1:MAX_RAND_PAD_LENGTH_MOD8)))

makepad(n::Integer) = zeros(UInt8, padding(n)-n)


function rand_primitive_buffer(;lpad=randpad(), rpad=randpad())
    dtype = rand(PRIMITIVE_ELTYPES)
    len = rand(1:MAX_VECTOR_LENGTH)
    v = rand(dtype, len)
    b = convert(Vector{UInt8}, reinterpret(UInt8, v))
    b = vcat(lpad, b, rpad)
    len, dtype, length(lpad), b, v
end

function rand_nullableprimitive_buffer(;lpad=randpad(), rpad=randpad())
    dtype = rand(PRIMITIVE_ELTYPES)
    len = rand(1:MAX_VECTOR_LENGTH)
    pres = rand(Bool, len)
    mask = Arrow.bitpackpadded(pres)
    vraw = rand(dtype, len)
    vreint = reinterpret(UInt8, vraw)
    b = vcat(lpad, mask, convert(Vector{UInt8}, vreint), rpad)
    v = Union{dtype,Missing}[pres[i] ? vraw[i] : missing for i ∈ 1:len]
    len, dtype, length(mask), length(lpad), b, v
end

# this one keeps values zero where possible
function rand_nullableprimitive_buffer2()
    dtype = rand(PRIMITIVE_ELTYPES)
    len = rand(1:MAX_VECTOR_LENGTH)
    pres = rand(Bool, len)
    mask = Arrow.bitpackpadded(pres)
    vraw = rand(dtype, len)
    for i ∈ 1:length(pres)
        pres[i] || (vraw[i] = zero(dtype))
    end
    vreint = reinterpret(UInt8, vraw)
    b = vcat(mask, convert(Vector{UInt8}, vreint), makepad(length(vreint)))
    v = Union{dtype,Missing}[pres[i] ? vraw[i] : missing for i ∈ 1:len]
    v, b
end

randmissing(::Type{T}) where {T} = rand(Bool) ? missing : rand(T)

randmissings(::Type{T}, len::Integer) where {T} = Union{T,Missing}[randmissing(T) for i ∈ 1:len]


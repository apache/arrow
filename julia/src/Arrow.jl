module Arrow

using CategoricalArrays, Dates


const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
const ALIGNMENT = 8


abstract type ArrowVector{T} <: AbstractVector{T} end


include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("arrowvectors.jl")
include("datetime.jl")
include("dictencoding.jl")
include("bitprimitives.jl")
include("locate.jl")


export ArrowVector
export padding, writepadded, bytesforbits, bitpack, bitpackpadded, unbitpack
export Primitive, NullablePrimitive, valuesbytes, bitmaskbytes, totalbytes, arrowview
export List, NullableList, offsetsbytes, offsets, getoffset
export values, bitmask, offsets, rawvalues, nullcount, isnull, arrowformat, writepadded,
    rawpadded
export Timestamp, TimeOfDay, Datestamp
export DictEncoding, referencetype, references
export BitPrimitive, NullableBitPrimitive
export Locate, locate


end  # module Arrow

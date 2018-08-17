VERSION < v"0.7.0-beta2.199" && __precompile__()
module Arrow

using CategoricalArrays, Compat, Compat.Dates


const BITMASK = UInt8[1, 2, 4, 8, 16, 32, 64, 128]
const ALIGNMENT = 8


import Base: getindex, setindex!
import Base: convert, show, unsafe_string, checkbounds, write, values, copy
import Base: length, size, eltype, getindex, isassigned, view
import Base: IndexStyle
import Base: >, ≥, <, ≤, ==
import CategoricalArrays: levels


if VERSION ≥ v"0.7.0-"
else
    using Missings
    import Base: isnull
end


abstract type ArrowVector{T} <: AbstractVector{T} end
export ArrowVector


include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("arrowvectors.jl")
include("datetime.jl")
include("dictencoding.jl")
include("bitprimitives.jl")
include("locate.jl")


end  # module Arrow

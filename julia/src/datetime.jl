
const UNIXEPOCH_TS = Dates.value(DateTime(1970))  # millisceonds
const UNIXEPOCH_DT = Dates.value(Date(1970))


value(x) = x


abstract type ArrowTime <: Dates.AbstractTime end

ArrowTime(t::Dates.TimeType) = convert(ArrowTime, t)

"""
    Timestamp{P<:Dates.TimePeriod} <: ArrowTime

Timestamp in which time is stored in units `P` as `Int64` for Arrow formatted data.
"""
struct Timestamp{P<:TimePeriod} <: ArrowTime
    value::Int64
end

Timestamp(t::P) where P<:TimePeriod = Timestamp{P}(Dates.value(t))
Timestamp{P}(t::DateTime) where P<:TimePeriod = convert(Timestamp{P}, t)
Timestamp(t::DateTime) = convert(Timestamp, t)

value(t::Timestamp) = t.value
unitvalue(t::Timestamp{P}) where P = P(value(t))

scale(::Type{D}, t::Timestamp{P}) where {D,P} = convert(D, unitvalue(t))

function Base.convert(::Type{DateTime}, t::Timestamp{P}) where P
    DateTime(Dates.UTM(UNIXEPOCH_TS + Dates.value(scale(Millisecond, t))))
end
Base.convert(::Type{TimeType}, t::Timestamp) = convert(DateTime, t)

function Base.convert(::Type{Timestamp{P}}, t::DateTime) where P
    Timestamp(convert(P, Millisecond(Dates.value(t) - UNIXEPOCH_TS)))
end
Base.convert(::Type{Timestamp}, t::DateTime) = convert(Timestamp{Millisecond}, t)
Base.convert(::Type{ArrowTime}, t::DateTime) = convert(Timestamp, t)

Base.show(io::IO, t::Timestamp) = show(io, convert(DateTime, t))


"""
    TimeOfDay{P<:Dates.TimePeriod,T<:Union{Int32,Int64}} <: ArrowTime

An arrow formatted object for representing the time of day.
Underlying data is `Int32` for seconds and milliseconds, `Int64` for microsecond and nanosecond.
"""
struct TimeOfDay{P<:TimePeriod,T<:Union{Int32,Int64}} <: ArrowTime
    value::T
end

function TimeOfDay{P,T}(t::P) where {P<:TimePeriod,T<:Union{Int32,Int64}}
    TimeOfDay{P,T}(Dates.value(convert(P, t)))
end
TimeOfDay{P}(t::P) where P<:Union{Second,Millisecond} = TimeOfDay{P,Int32}(t)
TimeOfDay{P}(t::P) where P<:Union{Microsecond,Nanosecond} = TimeOfDay{P,Int64}(t)
TimeOfDay(t::P) where P<:TimePeriod = TimeOfDay{P}(t)
TimeOfDay(t::Time) = convert(TimeOfDay, t)

value(t::TimeOfDay) = t.value
unitvalue(t::TimeOfDay{P}) where P = P(value(t))

scale(::Type{D}, t::TimeOfDay{P}) where {D,P} = convert(D, unitvalue(t))

function Base.convert(::Type{Time}, t::TimeOfDay{P}) where P
    Time(Nanosecond(scale(Nanosecond, t)))
end
Base.convert(::Type{TimeType}, t::TimeOfDay) = convert(Time, t)

Base.convert(::Type{TimeOfDay{P,T}}, t::Time) where {P,T} = TimeOfDay{P,T}(convert(P, t.instant))
Base.convert(::Type{TimeOfDay{P}}, t::Time) where P = TimeOfDay{P}(convert(P, t.instant))
Base.convert(::Type{TimeOfDay}, t::Time) = convert(TimeOfDay{Nanosecond}, t)
Base.convert(::Type{ArrowTime}, t::Time) = convert(TimeOfDay, t)

Base.show(io::IO, t::TimeOfDay) = show(io, convert(Time, t))


"""
    Datestamp <: ArrowTime

Stores a date as an `Int32` for Arrow formatted data.
"""
struct Datestamp <: ArrowTime
    value::Int32
end

Datestamp(t::Date) = convert(Datestamp, t)

value(t::Datestamp) = t.value

Base.convert(::Type{Date}, t::Datestamp) = Date(Dates.UTD(UNIXEPOCH_DT + value(t)))
Base.convert(::TimeType, t::Datestamp) = convert(Date, t)

Base.convert(::Type{Datestamp}, t::Date) = Datestamp(Dates.value(t) - UNIXEPOCH_DT)
Base.convert(::Type{ArrowTime}, t::Date) = convert(Datestamp, t)

Base.show(io::IO, t::Datestamp) = show(io, convert(Date, t))


#======================================================================================================
    some basic utilities for dates and datetimes...
======================================================================================================#
for symb ∈ [:(Base.:(>)), :(Base.:(≥)), :(Base.:(<)), :(Base.:(≤)), :(Base.:(==))]
    @eval $symb(t1::T, t2::T) where {T<:ArrowTime} = $symb(value(t1), value(t2))
end

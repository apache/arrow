import Dates
import TimeZones

struct WrappedDate
    x::Dates.Date
end
Arrow.ArrowTypes.registertype!(WrappedDate, WrappedDate)

struct WrappedTime
    x::Dates.Time
end
Arrow.ArrowTypes.registertype!(WrappedTime, WrappedTime)

struct WrappedDateTime
    x::Dates.DateTime
end
Arrow.ArrowTypes.registertype!(WrappedDateTime, WrappedDateTime)

struct WrappedZonedDateTime
    x::TimeZones.ZonedDateTime
end
Arrow.ArrowTypes.registertype!(WrappedZonedDateTime, WrappedZonedDateTime)


@testset "Date and time wrappers with missing" begin
    for T in (WrappedDate, WrappedTime, WrappedDateTime, WrappedZonedDateTime)
        if T == WrappedZonedDateTime
            time = T(Dates.now(TimeZones.tz"UTC"))
        else
            time = T(Dates.now())
        end
        table = (; x = [missing, missing, time, missing, time])
        io = IOBuffer()
        Arrow.write(io, table)
        seekstart(io)
        tbl = Arrow.Table(io)
        @test isequal(collect(tbl.x), table.x)
    end
end

@testset "`default(T) isa T`" begin
    for T in (Dates.Date, Dates.Time, Dates.DateTime, TimeZones.ZonedDateTime, Dates.Nanosecond, Dates.Millisecond, Dates.Second, Dates.Day, Dates.Month, Dates.Year)
        @test Arrow.ArrowTypes.default(T) isa T
    end
end

using Arrow, Tables, Test

include(joinpath(dirname(pathof(Arrow)), "../test/arrowjson.jl"))
# using .ArrowJSON

function runcommand(jsonname, arrowname, mode, verbose)
    if jsonname == ""
        error("must provide json file name")
    end
    if arrowname == ""
        error("must provide arrow file name")
    end

    if mode == "ARROW_TO_JSON"
        tbl = Arrow.Table(arrowname)
        df = ArrowJSON.DataFile(tbl)
        open(jsonname, "w") do io
            JSON3.write(io, df)
        end
    elseif mode == "JSON_TO_ARROW"
        df = ArrowJSON.parsefile(jsonname)
        open(arrowname, "w") do io
            Arrow.write(io, df)
        end
    elseif mode == "VALIDATE"
        df = ArrowJSON.parsefile(jsonname)
        tbl = Arrow.Table(arrowname)
        @test isequal(df, tbl)
    else
        error("unknown integration test mode: $mode")
    end
    return
end

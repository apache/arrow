# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

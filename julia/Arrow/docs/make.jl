using Documenter
using Arrow


makedocs(;
    modules=[Arrow],
    repo="https://github.com/JuliaData/Arrow.jl/blob/{commit}{path}#L{line}",
    sitename="Arrow.jl",
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://JuliaData.github.io/Arrow.jl",
        assets=String[],
    ),
    pages = [
        "Home" => "index.md",
        "User Manual" => "manual.md",
        "API Reference" => "reference.md"
    ]
)

deploydocs(;
    repo="github.com/JuliaData/Arrow.jl",
    devbranch = "main"
)

using Documenter, DocumenterMarkdown
using Arrow

makedocs(
    format = Markdown(),
    modules = [Arrow],
    pages = [
        "Home" => "index.md",
        "User Manual" => "manual.md",
        "API Reference" => "reference.md"
    ]
)

deploydocs(repo = "github.com/JuliaData/Arrow.jl.git")

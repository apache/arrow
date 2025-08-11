Thank you for submitting a PR!

Before you continue, please ensure that your PR title and description (this message!) follow [conventional commit syntax](1). Substrait uses an automated release process that, among other things, uses PR titles & descriptions to build a changelog, so the syntax and format matter!

The title of the PR should be a valid commit header.

Some examples of proper commit message headers and PR titles:

 - `feat: add feature X`
 - `fix: X in case of Y`
 - `docs: improve documentation for X`

Note the case and grammar conventions.

Furthermore, the description of any PR that includes a breaking change should contain a paragraph that starts with `BREAKING CHANGE: ...`, where `...` explains what changed. The automated release process uses this to determine how it should bump the version number. Anything that changes the behavior of a plan that was previously legal is considered a breaking change; note that this includes behavior specifications that only exist in Substrait in the form of behavior descriptions on the website or in comments.

[1]: https://www.conventionalcommits.org/en/v1.0.0/

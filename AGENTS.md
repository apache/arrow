<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# AGENTS.md

This file provides guidance for AI coding agents working on Apache Arrow.

Apache Arrow is a multi-language toolbox for accelerated data interchange
and in-memory processing.

## Repository Layout

| Directory | Description |
|-----------|-------------|
| `cpp/` | Core C++ library (libarrow, Parquet, Flight, Gandiva, etc.) |
| `python/` | PyArrow (Python bindings for libarrow) |
| `r/` | R package |
| `c_glib/` | C/GLib bindings |
| `ruby/` | Ruby bindings |
| `matlab/` | MATLAB implementation |
| `docs/` | Sphinx documentation source |
| `ci/` | CI configuration (Docker, scripts) |
| `dev/` | Developer tools and release scripts |
| `format/` | Arrow columnar format specification |
| `testing/` | Shared test data (integration testing) |

Several implementations have moved to their own repositories:

- **Java**: [apache/arrow-java](https://github.com/apache/arrow-java)
- **Go**: [apache/arrow-go](https://github.com/apache/arrow-go)
- **Rust**: [apache/arrow-rs](https://github.com/apache/arrow-rs)
- **C#**: [apache/arrow-csharp](https://github.com/apache/arrow-csharp)
- **JavaScript**: [apache/arrow-js](https://github.com/apache/arrow-js)
- **Swift**: [apache/arrow-swift](https://github.com/apache/arrow-swift)
- **Julia**: [apache/arrow-julia](https://github.com/apache/arrow-julia)

## License Headers

All new source files must include the Apache License 2.0 header.
Copy the header format from an existing file in the same directory.

## Cross-Language Dependencies

The C++ library (`cpp/`) is the shared core. PyArrow, R, and MATLAB
bind directly to it. C/GLib wraps the C++ library, and Ruby binds to
C/GLib. Changes in `cpp/` can break any of these downstream components.
When modifying C++ code, consider the impact on dependent bindings.

## Git Conventions

### Remote Names

- `upstream` → `apache/arrow` (fetch from here)
- `origin` → your fork (push PR branches here)

Verify with `git remote -v` before pushing. Never push directly to
`upstream`.

### Commit Messages

Use imperative mood and focus on *why*, not *what*.

Good: `Fix UnionArray validation when type codes are non-contiguous`
Bad: `Fixed the bug in union array`

## Issue Conventions

- Follow the issue templates at
  https://github.com/apache/arrow/tree/main/.github/ISSUE_TEMPLATE
  when creating a new issue.
- Do NOT include a `GH-` prefix in issue titles (that is only for PRs).
- Search existing issues before creating a new one.

## Pull Request Conventions

### Title Format

```
GH-<issue-number>: [Component] Short description
```

Example: `GH-14736: [C++][Python] Ensure no validity bitmap in UnionArray::SetData`

### MINOR Changes

For trivial documentation or typo fixes affecting no more than 2 files and
no more than 500 words, you may skip creating an issue and prefix the PR
title with `MINOR: ` instead:

    MINOR: [Docs] Fix typo in memory.rst

Do NOT combine `MINOR: ` with a `GH-` number.

### PR Description

Fill in all four sections of the PR template at
https://github.com/apache/arrow/blob/main/.github/pull_request_template.md

Descriptions should be concise and to the point — do not restate things
that are obvious from reading the diff.

### AI-Generated Content

Never open pull requests or post comments to GitHub automatically.
A human must review all changes locally and write the PR description
before submitting.

## Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
pre-commit run -a                        # all hooks
pre-commit run --all-files <hook-alias>   # e.g. python, cpp (see aliases in .pre-commit-config.yaml)
```

Always run the relevant pre-commit checks before submitting a PR.

## Testing

Run relevant tests locally before submitting a PR. See the
[Testing](https://arrow.apache.org/docs/developers/guide/step_by_step/testing.html)
section of the contributor guide for component-specific instructions.

## AI-Generated Code Policy

See: https://arrow.apache.org/docs/dev/developers/overview.html#ai-generated-code

## Component-Specific Guides

Some subdirectories may contain their own `AGENTS.md` with language-specific
build instructions, testing commands, and coding conventions. Check the
relevant component directory before starting work.

## Further Resources

- [New Contributor's Guide](https://arrow.apache.org/docs/developers/guide/index.html)
- [Contributing Overview](https://arrow.apache.org/docs/developers/overview.html)
- [CONTRIBUTING.md](CONTRIBUTING.md)

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

## Issue Conventions

- Prefix the issue title with the component name in brackets:
  `[C++][Python] Short description of the problem`
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
title with `MINOR:` instead:

```
MINOR: [Docs] Fix typo in memory.rst
```

Do NOT combine `MINOR:` with a `GH-` number.

### PR Description

Fill in all four sections of the PR template:

1. **Rationale for this change** - Why is this needed?
2. **What changes are included in this PR?** - What did you do?
3. **Are these changes tested?** - How are they validated?
4. **Are there any user-facing changes?** - Yes/No with brief explanation

Descriptions should be concise and to the point — do not restate things
that are obvious from reading the diff.

Only keep the "Critical Fix" or "Breaking changes" flags in the description
if they genuinely apply. Remove them otherwise.

## Pre-commit Hooks

```bash
pip install pre-commit
pre-commit install
pre-commit run -a                        # all hooks
pre-commit run --all-files <component>   # single component (alias from .pre-commit-config.yaml)
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

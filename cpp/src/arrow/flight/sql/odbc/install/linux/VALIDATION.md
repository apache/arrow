# Apache Arrow Flight SQL ODBC 25.0.1 Linux validation

Validation date: 2026-09-09

Source commit: `beccec0d0c451b7aa3e4530416ac431b3c035c69`

Source tag: `apache-arrow-25.0.1`

## Outcome

| Check | Result |
|---|---|
| Exact source and clean starting worktree | PASS |
| Repository-defined Ubuntu 24.04 amd64 build | PASS |
| x86_64 ELF and exported ODBC entry points | PASS |
| Dynamic dependency resolution | PASS |
| TLS CA-chain and hostname verification | PASS |
| Isolated unixODBC registration | PASS |
| Authenticated `SELECT 1` against Dremio Cloud | PASS, 3/3 |
| Cursor, statement, connection, and environment cleanup | PASS, 3/3 |
| Installer idempotence and registration ownership guards | PASS |
| Reproducible archive, checksum, install, load, and uninstall | PASS |
| Credential file removal | PASS |

## Environment

- Container image:
  `apache/arrow-dev@sha256:a887c3bfb5262539c9046d414a8dfa9390c65358019e760128e8104e86041b36`
- Container architecture: `x86_64` / Debian architecture `amd64`.
- Distribution: Ubuntu 24.04.4 LTS.
- Toolchain: GCC/G++ 13.3.0, CMake 3.28.3, Ninja 1.11.1.
- Runtime: glibc 2.39 and unixODBC 2.3.12.
- Execution host: Apple Silicon, using an aarch64 Colima VM and Rosetta to run
  the complete amd64 container userspace.

The compiler, linker, unixODBC driver manager, driver, and smoke-test process
were x86_64. This validates the Linux x86_64 ABI and end-to-end behavior, but it
is not a native x86_64 performance result.

## Build evidence

The build used the same Ubuntu version, architecture, bundled-dependency mode,
and disabled shared dependency linkage as Arrow's `odbc-linux` CI job and
`ubuntu-cpp-odbc` Compose service. CMake reported Arrow 25.0.1, x86_64, and the
Release configuration. The build completed the `install` target successfully.
It began at four-way parallelism and resumed the same Ninja graph at eight-way
parallelism after an intentional clean interrupt; no configuration or source
changed at that boundary.

The unstripped result was 60.4 MiB and identified as:

```text
ELF 64-bit LSB shared object, x86-64
Machine: Advanced Micro Devices X86-64
Type: DYN (Shared object file)
```

The driver exported the expected Unix wide-character entry points, including
`SQLConnectW`, `SQLDriverConnectW`, and `SQLExecDirectW`, plus common entry
points such as `SQLDisconnect`, `SQLFetch`, and `SQLCloseCursor`.

Direct dynamic dependencies were `libcurl.so.4`, `libodbcinst.so.2`,
`libstdc++.so.6`, `libm.so.6`, `libgcc_s.so.1`, `libc.so.6`, and the x86_64
loader. `ldd` resolved those and every transitive dependency. No dynamic Arrow,
Flight, Flight SQL, gRPC, or Protobuf library was required.

## TLS, registration, and query evidence

An independent OpenSSL preflight against `data.eu.dremio.cloud:443`, with SNI,
`-verify_hostname`, and `-verify_return_error`, reported `Verification: OK` and
`Verify return code: 0 (ok)`.

The driver was then registered in an isolated mode-0600 `odbcinst.ini`. The
registered `Driver` and `Setup` values both resolved to the just-built shared
library; no host or container system ODBC configuration was modified.

The compiled smoke client performed three independent cycles. Every cycle
reported:

```text
connection: success (TLS certificate verification enabled)
query: SELECT 1
result: 1
cleanup: close cursor: success
cleanup: free statement: success
cleanup: disconnect: success
cleanup: free connection: success
cleanup: free environment: success
smoke test: PASS
```

This directly covers the connect, execute, fetch, cursor close, disconnect, and
handle-destruction path that a compile-only check would miss.

## Packaging decision

With `ARROW_FLIGHT_SQL_ODBC_INSTALLER=ON`, the pinned source prints:

```text
ODBC_PACKAGE_FORMAT DEB not implemented, see GH-49595
ODBC_PACKAGE_FORMAT RPM not implemented, see GH-47977
```

The Linux branch sets no DEB or RPM generator and contains the additional TODO
to create a Linux installer. Manually forcing CPack's generic TGZ generator did
produce an archive, but inspection showed a generic Arrow library layout and no
unixODBC registration scripts. It is not a complete Linux ODBC installation
artifact.

The validated deliverable is therefore the versioned x86_64 driver plus a
relocatable tar archive with explicit `install.sh` and `uninstall.sh`, an
`odbcinst.ini` template, the smoke-test source and binary, licenses, this report,
and the build runbook. `SHA256SUMS` authenticates both deliverables.

The stripped direct driver was 47 MiB and the archive was 16 MiB. Two packaging
runs produced identical SHA-256 values. The exact archive then passed checksum
verification, extraction, installation at the default prefix, repeat
installation with `UsageCount=1`, dynamic loading, documentation installation,
unregistration, and exact-prefix file removal.

## Installer and credential safety

The installer tests used an isolated unixODBC configuration and verified:

- initial registration succeeds;
- reinstall at the same prefix is idempotent and leaves `UsageCount=1`;
- install refuses to replace a same-name driver registered at another path;
- uninstall refuses to remove a same-name driver registered at another path;
- uninstall removes its own registration and exact installed files.

The Dremio token was never placed in a repository file, ODBC configuration,
command-line argument, or artifact. It was read from a root-owned mode-0600
container secret file. Diagnostic text was redacted by the smoke client, driver
logging was disabled, and a shell trap deleted the secret file immediately after
the three live attempts. A follow-up existence check passed. Because the token
was originally supplied through an interactive conversation, it should still be
revoked after validation.

## Limitations

- Functional and ABI behavior was validated under translated x86_64 container
  execution rather than bare-metal x86_64 Linux.
- This is not a performance, load, failover, or broad SQL conformance test.
- Compatibility with distributions older than Ubuntu 24.04 is not established;
  the artifact should be treated as an Ubuntu 24.04 / glibc 2.39 build.

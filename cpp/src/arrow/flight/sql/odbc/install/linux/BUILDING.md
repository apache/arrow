# Apache Arrow Flight SQL ODBC 25.0.1: Linux build and install

This runbook builds the Apache Arrow Flight SQL ODBC driver from Arrow commit
`beccec0d0c451b7aa3e4530416ac431b3c035c69` and produces an x86_64 Linux shared
library plus a relocatable tar archive. It never puts a Dremio credential in a
source file, command-line argument, shell history, log, or artifact.

## Tested target

- Target ABI: Linux x86_64 (`ELF 64-bit LSB shared object, x86-64`).
- Distribution: Ubuntu 24.04 LTS.
- Toolchain: GCC/G++ 13.3.0, CMake 3.28.3, Ninja 1.11.1, glibc 2.39, and
  unixODBC 2.3.12.
- Build mode: Release, shared Arrow Flight SQL ODBC driver with Arrow, Flight,
  Flight SQL, gRPC, and Protobuf linked statically. libcurl, libodbcinst, the C++
  runtime, and the standard system libraries remain dynamic.

The repository's `.github/workflows/cpp_extra.yml` defines its `odbc-linux` CI
job as `amd64` on Ubuntu 24.04. The corresponding `ubuntu-cpp-odbc` service in
`compose.yaml` uses bundled dependencies and disables shared dependency linkage.
That is the repository-defined CI target used here.

The recorded validation used an amd64 Ubuntu container executed through Rosetta
inside an aarch64 Colima Linux VM on Apple Silicon. The compiler, linker,
unixODBC driver manager, driver, smoke-test process, and userspace were all
x86_64. This is an x86_64 ABI validation, but it is not a bare-metal x86_64
performance test.

## Prerequisites

On a native Ubuntu 24.04 x86_64 host, install Git, Docker Engine with Compose,
and CA certificates. Allow roughly 12 GiB RAM and 30 GiB free disk space because
the Unix driver build compiles bundled static dependencies.

On Apple Silicon, a disposable Colima VM can provide amd64 container execution:

```bash
brew install colima docker docker-compose
colima start arrow-odbc-linux \
  --arch aarch64 --vm-type vz --vz-rosetta \
  --cpu 10 --memory 12 --disk 80
```

No AWS EC2 resource is required by this procedure.

## Verify the source

```bash
git switch --detach beccec0d0c451b7aa3e4530416ac431b3c035c69
test "$(git rev-parse HEAD)" = beccec0d0c451b7aa3e4530416ac431b3c035c69
git status --short
```

Do not continue from a dirty worktree unless the changes are understood and
intended. The commands below write build products only under
`cpp/build/linux-odbc-validation`.

## Build the x86_64 driver

Build the repository-provided Ubuntu 24.04 toolchain image:

```bash
ARCH=amd64 ARCH_SHORT=amd64 UBUNTU=24.04 \
  docker-compose build ubuntu-cpp-odbc
```

Build and install into the disposable container. The build directory is mounted
back to the host so the driver remains available after the container exits.

```bash
mkdir -p cpp/build/linux-odbc-validation
docker run --rm --platform linux/amd64 \
  -v "$PWD:/arrow" \
  -v "$PWD/cpp/build/linux-odbc-validation:/build" \
  -e ARROW_ACERO=OFF \
  -e ARROW_AZURE=OFF \
  -e ARROW_BUILD_PARALLEL=4 \
  -e ARROW_BUILD_SHARED=ON \
  -e ARROW_BUILD_STATIC=ON \
  -e ARROW_BUILD_TESTS=OFF \
  -e ARROW_BUILD_TYPE=release \
  -e ARROW_BUILD_UTILITIES=OFF \
  -e ARROW_CSV=OFF \
  -e ARROW_DATASET=OFF \
  -e ARROW_DEPENDENCY_SOURCE=BUNDLED \
  -e ARROW_DEPENDENCY_USE_SHARED=OFF \
  -e ARROW_FLIGHT=ON \
  -e ARROW_FLIGHT_SQL=ON \
  -e ARROW_FLIGHT_SQL_ODBC=ON \
  -e ARROW_FLIGHT_SQL_ODBC_INSTALLER=OFF \
  -e ARROW_GANDIVA=OFF \
  -e ARROW_GCS=OFF \
  -e ARROW_HDFS=OFF \
  -e ARROW_HOME=/usr/local \
  -e ARROW_JEMALLOC=OFF \
  -e ARROW_MIMALLOC=OFF \
  -e ARROW_ORC=OFF \
  -e ARROW_PARQUET=OFF \
  -e ARROW_S3=OFF \
  -e ARROW_SUBSTRAIT=OFF \
  -e ARROW_USE_CCACHE=OFF \
  -e CMAKE_BUILD_PARALLEL_LEVEL=4 \
  apache/arrow-dev:amd64-ubuntu-24.04-cpp \
  /arrow/ci/scripts/cpp_build.sh /arrow /build
```

The unstripped build result is:

```text
cpp/build/linux-odbc-validation/cpp/release/libarrow_flight_sql_odbc.so
```

## Package and checksum

The pinned source does not implement a Linux DEB or RPM. In
`cpp/src/arrow/flight/sql/odbc/CMakeLists.txt`, the Linux installer branch emits
explicit TODO messages for both formats, leaves the Linux ODBC and documentation
install directories unset, and selects no Linux CPack generator. Therefore the
validated deliverable is a relocatable tar archive, not a DEB or RPM.

Create the stripped direct library, tar archive, smoke-test binary, and
`SHA256SUMS` in the Linux-only artifact directory:

```bash
linux_dir=cpp/src/arrow/flight/sql/odbc/install/linux
mkdir -p "${linux_dir}/artifacts"
docker run --rm --platform linux/amd64 \
  -v "$PWD:/arrow" -w /arrow \
  apache/arrow-dev:amd64-ubuntu-24.04-cpp \
  "${linux_dir}/package.sh" \
  /arrow/cpp/build/linux-odbc-validation/cpp/release/libarrow_flight_sql_odbc.so \
  "/arrow/${linux_dir}/artifacts"

cd "${linux_dir}/artifacts"
sha256sum --check SHA256SUMS
cd -
```

## Install and register with unixODBC

Ubuntu runtime prerequisites are the `unixodbc`, `odbcinst`, `libcurl4t64`, and
`ca-certificates` packages, plus the standard C/C++ runtime and the transitive
libraries reported by `ldd`. Verify the artifact before install:

```bash
sha256sum --check SHA256SUMS
tar -xzf apache-arrow-flight-sql-odbc-25.0.1-linux-x86_64.tar.gz
cd apache-arrow-flight-sql-odbc-25.0.1-linux-x86_64
ldd lib/libarrow_flight_sql_odbc.so
sudo ./install.sh
odbcinst -q -d -n 'Apache Arrow Flight SQL ODBC Driver'
```

`install.sh` defaults to `/opt/apache-arrow-flight-sql-odbc/25.0.1` and uses
`odbcinst` to register the absolute library path. Pass a different absolute
prefix as its sole argument if required. Re-running it at the same prefix is
idempotent. It refuses to overwrite a same-name registration at another path;
`uninstall.sh` likewise unregisters only the exact path it owns.

For a non-root, isolated validation, avoid changing `/etc/odbcinst.ini`:

```bash
package_root="$PWD/apache-arrow-flight-sql-odbc-25.0.1-linux-x86_64"
mkdir -p "$PWD/odbc-config"
sed "s|@DRIVER_PATH@|${package_root}/lib/libarrow_flight_sql_odbc.so|g" \
  "${package_root}/odbcinst.ini.in" >"$PWD/odbc-config/odbcinst.ini"
export ODBCSYSINI="$PWD/odbc-config"
export ODBCINSTINI=odbcinst.ini
odbcinst -q -d -n 'Apache Arrow Flight SQL ODBC Driver'
```

## Run the Dremio Cloud smoke test securely

The smoke test defaults to `data.eu.dremio.cloud:443`, enables TLS, verifies the
server certificate against the Linux system trust store, and executes
`SELECT 1`. For token authentication, it prefers a short-lived token from the
file named by `DREMIO_ODBC_TOKEN_FILE`. For user/password authentication, it
reads `DREMIO_ODBC_UID` and prefers the secret from
`DREMIO_ODBC_PASSWORD_FILE`. The connection string exists only in process
memory and is never printed.

Have the secret manager materialize a short-lived secret as a mode-0600 file,
then export only its path. Do not paste the secret into these commands.

```bash
export DREMIO_ODBC_TOKEN_FILE=/run/secrets/dremio_odbc_token
export DREMIO_ODBC_HOST=data.eu.dremio.cloud
export DREMIO_ODBC_PORT=443

./smoke/flight_sql_odbc_smoke_test
```

Expected evidence includes `connection: success`, `result: 1`, successful cursor,
statement, connection, and environment cleanup lines, and `smoke test: PASS`.
A successful compile or driver registration alone is not a successful remote
smoke test.

After the test, revoke the short-lived credential and remove the secret file
through the secret manager. Then clear the process environment:

```bash
unset DREMIO_ODBC_TOKEN_FILE DREMIO_ODBC_UID DREMIO_ODBC_PASSWORD_FILE \
  DREMIO_ODBC_HOST DREMIO_ODBC_PORT
```

## Uninstall and tear down

From the extracted package directory:

```bash
sudo ./uninstall.sh
if odbcinst -q -d -n 'Apache Arrow Flight SQL ODBC Driver'; then
  echo 'driver registration still exists' >&2
  exit 1
fi
```

Remove only the disposable Colima profile created for this validation:

```bash
colima stop arrow-odbc-linux
colima delete arrow-odbc-linux
```

If a cloud VM is substituted for Colima, record its instance ID, security group,
key, and volume IDs before use, then terminate the instance and delete only those
recorded resources. Confirm that no volume, elastic IP, or security group remains.

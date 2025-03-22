#!/bin/bash
set -ex

# temporary prefix to be able to install files more granularly
mkdir temp_prefix

cmake --install ./cpp/build --prefix=./temp_prefix

if [[ "${PKG_NAME}" == "libarrow" ]]; then
    # only libarrow (+ activation scripts)
    cp -a ./temp_prefix/lib/libarrow.* $PREFIX/lib
    cp -a ./temp_prefix/lib/libarrow_cuda.* $PREFIX/lib || true
    cp ./temp_prefix/lib/pkgconfig/arrow.pc $PREFIX/lib/pkgconfig
    cp ./temp_prefix/lib/pkgconfig/arrow-compute.pc $PREFIX/lib/pkgconfig
    cp ./temp_prefix/lib/pkgconfig/arrow-csv.pc $PREFIX/lib/pkgconfig
    cp ./temp_prefix/lib/pkgconfig/arrow-cuda.pc $PREFIX/lib/pkgconfig || true
    cp ./temp_prefix/lib/pkgconfig/arrow-filesystem.pc $PREFIX/lib/pkgconfig
    cp ./temp_prefix/lib/pkgconfig/arrow-json.pc $PREFIX/lib/pkgconfig
    cp ./temp_prefix/lib/pkgconfig/arrow-orc.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/Arrow/. $PREFIX/lib/cmake/Arrow
    cp -R ./temp_prefix/lib/cmake/ArrowCUDA/. $PREFIX/lib/cmake/ArrowCUDA || true
    cp -R ./temp_prefix/share/arrow/. $PREFIX/share/arrow
    cp -R ./temp_prefix/share/doc/. $PREFIX/share/doc
    cp -R ./temp_prefix/share/gdb/. $PREFIX/share/gdb
    cp -R ./temp_prefix/include/arrow/. $PREFIX/include/arrow

    # Copy the [de]activate scripts to $PREFIX/etc/conda/[de]activate.d, see
    # https://conda-forge.org/docs/maintainer/adding_pkgs.html#activate-scripts
    for CHANGE in "activate"
    do
        mkdir -p "${PREFIX}/etc/conda/${CHANGE}.d"
        cp "${RECIPE_DIR}/${CHANGE}.sh" "${PREFIX}/etc/conda/${CHANGE}.d/${PKG_NAME}_${CHANGE}.sh"
    done
elif [[ "${PKG_NAME}" == "libarrow-acero" ]]; then
    # only libarrow-acero
    cp -a ./temp_prefix/lib/libarrow_acero.* $PREFIX/lib
    cp ./temp_prefix/lib/pkgconfig/arrow-acero.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/ArrowAcero/. $PREFIX/lib/cmake/ArrowAcero
elif [[ "${PKG_NAME}" == "libarrow-dataset" ]]; then
    # only libarrow-dataset
    cp -a ./temp_prefix/lib/libarrow_dataset.* $PREFIX/lib
    cp ./temp_prefix/lib/pkgconfig/arrow-dataset.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/ArrowDataset/. $PREFIX/lib/cmake/ArrowDataset
elif [[ "${PKG_NAME}" == "libarrow-flight" ]]; then
    # only libarrow-flight
    cp -a ./temp_prefix/lib/libarrow_flight.* $PREFIX/lib
    cp ./temp_prefix/lib/libarrow_flight_transport_ucx.* $PREFIX/lib || true
    cp ./temp_prefix/lib/pkgconfig/arrow-flight.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/ArrowFlight/. $PREFIX/lib/cmake/ArrowFlight
elif [[ "${PKG_NAME}" == "libarrow-flight-sql" ]]; then
    # only libarrow-flight-sql
    cp -a ./temp_prefix/lib/libarrow_flight_sql.* $PREFIX/lib
    cp ./temp_prefix/lib/pkgconfig/arrow-flight-sql.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/ArrowFlightSql/. $PREFIX/lib/cmake/ArrowFlightSql
elif [[ "${PKG_NAME}" == "libarrow-gandiva" ]]; then
    # only libarrow-gandiva
    cp -a ./temp_prefix/lib/libgandiva.* $PREFIX/lib
    cp ./temp_prefix/lib/pkgconfig/gandiva.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/Gandiva/. $PREFIX/lib/cmake/Gandiva
    cp -R ./temp_prefix/include/gandiva/. $PREFIX/include/gandiva
elif [[ "${PKG_NAME}" == "libarrow-substrait" ]]; then
    # only libarrow-substrait
    cp -a ./temp_prefix/lib/libarrow_substrait.* $PREFIX/lib
    cp ./temp_prefix/lib/pkgconfig/arrow-substrait.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/ArrowSubstrait/. $PREFIX/lib/cmake/ArrowSubstrait
elif [[ "${PKG_NAME}" == "libparquet" ]]; then
    # only parquet
    cp -a ./temp_prefix/lib/libparquet.* $PREFIX/lib
    cp ./temp_prefix/lib/pkgconfig/parquet.pc $PREFIX/lib/pkgconfig
    cp -R ./temp_prefix/lib/cmake/Parquet/. $PREFIX/lib/cmake/Parquet
    cp -R ./temp_prefix/include/parquet/. $PREFIX/include/parquet
elif [[ "${PKG_NAME}" == "libarrow-all" ]]; then
    # libarrow-all: install everything else (whatever ends up in this output
    # should generally be installed into the appropriate libarrow-<flavour>).
    cmake --install ./cpp/build --prefix=$PREFIX
else
    # shouldn't happen
    exit 1
fi

if [[ "${PKG_NAME}" != "libarrow" ]]; then
    # delete symlink that's created by libarrow's activation script,
    # to avoid that it gets wrongly detected as content of libarrow-*.
    rm $PREFIX/share/gdb/auto-load/$PREFIX/lib/libarrow.*-gdb.py || true
fi

# Clean up temp_prefix
rm -rf temp_prefix

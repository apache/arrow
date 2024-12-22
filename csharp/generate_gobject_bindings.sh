#!/bin/bash

set -e

if [ -z "${ARROW_HOME}" ]; then
    echo "ARROW_HOME must be set"
    exit 1
fi

csharp_dir=$(dirname $0)
source_dir="${csharp_dir}/src"
gir_dir="${csharp_dir}/gir_files"

gircore_gir_dir="${csharp_dir}/tools/gir.core/ext/gir-files"
arrow_gir_dir="${ARROW_HOME}/share/gir-1.0"

gir_dependencies=(GObject-2.0 GLib-2.0 Gio-2.0 GModule-2.0)
arrow_gobject_libs=(Arrow-1.0 ArrowDataset-1.0)
namespaces=(Apache.Arrow.GLibBindings Apache.Arrow.Dataset.GLibBindings)

os_names=(linux macos windows)
for os_name in ${os_names[@]}; do
    mkdir -p "${gir_dir}/${os_name}"
    for gir_dep in ${gir_dependencies[@]}; do
        cp "${gircore_gir_dir}/${os_name}/${gir_dep}.gir" "${gir_dir}/${os_name}"
    done

done

# TODO: Make this select the appropriate platform and merge all platforms in CI build
os_name="linux"
for index in ${!arrow_gobject_libs[*]}; do
    arrow_lib=${arrow_gobject_libs[$index]}
    cp "${arrow_gir_dir}/${arrow_lib}.gir" "${gir_dir}/${os_name}"
    namespace=${namespaces[$index]}
    sed -i.bak "s/<namespace name=\"\([^\"]*\)\"/<namespace name=\"${namespace}\"/" "${gir_dir}/${os_name}/${arrow_lib}.gir"
    rm -r "${gir_dir}/${os_name}/${arrow_lib}.gir.bak"
done

dotnet run --project tools/gir.core/src/Generation/GirTool/GirTool.csproj -- \
    generate Arrow-1.0.gir ArrowDataset-1.0.gir \
    --search-path-linux "${gir_dir}/linux" \
    --search-path-windows "${gir_dir}/windows" \
    --search-path-macos "${gir_dir}/macos" \
    --output "${source_dir}"

# We use gobject libraries from gir.core NuGet packages so delete generated source for these
for gir_dep in ${gir_dependencies[@]}; do
    rm -r "${source_dir}/${gir_dep}"
done

# Apply patches to fix generated code
pushd "${csharp_dir}"
patch -p2 < ./glib_generated_fixes.patch
popd

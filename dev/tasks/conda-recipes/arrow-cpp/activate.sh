#!/bin/bash

# for the gdb-wrappers, we need to create a symlink that
# contains the full path of the lib _within_ the installed
# env, which we don't have until the env is created.

# doesn't come with a deactivate script, because the symlink
# is benign and doesn't need to be deleted.

_la_log() {
    if [ "${CF_LIBARROW_ACTIVATE_LOGGING:-}" = "1" ]; then
        # The following loop is necessary to handle multi-line strings
        # like for the output of `ls -al`.
        printf '%s\n' "$*" | while IFS= read -r line
        do
            echo "$CONDA_PREFIX/etc/conda/activate.d/libarrow_activate.sh DEBUG: $line"
        done
    fi
}

_la_log "Beginning libarrow activation."

# where the GDB wrappers get installed
_la_gdb_prefix="$CONDA_PREFIX/share/gdb/auto-load"

# If the directory is not writable, nothing can be done
if [ ! -w "$_la_gdb_prefix" ]; then
    _la_log 'No rights to modify $_la_gdb_prefix, cannot create symlink!'
    _la_log 'Unless you plan to use the GDB debugger with libarrow, this warning can be safely ignored.'
    return
fi

# this needs to be in sync with ARROW_GDB_INSTALL_DIR in build.sh
_la_placeholder="replace_this_section_with_absolute_slashed_path_to_CONDA_PREFIX"
# the paths here are intentionally stacked, see #935, resp.
# https://github.com/apache/arrow/blob/master/docs/source/cpp/gdb.rst#manual-loading
_la_symlink_dir="$_la_gdb_prefix/$CONDA_PREFIX/lib"
_la_orig_install_dir="$_la_gdb_prefix/$_la_placeholder/lib"

_la_log "          _la_gdb_prefix: $_la_gdb_prefix"
_la_log "         _la_placeholder: $_la_placeholder"
_la_log "         _la_symlink_dir: $_la_symlink_dir"
_la_log "    _la_orig_install_dir: $_la_orig_install_dir"
_la_log "  content of that folder:"
_la_log "$(ls -al "$_la_orig_install_dir" | sed 's/^/      /')"

# there's only one lib in the _la_orig_install_dir folder, but the libname changes
# based on the version so use a loop instead of hardcoding it.
for _la_target in "$_la_orig_install_dir/"*.py; do
    if [ ! -e "$_la_target" ]; then
        # If the file doesn't exist, skip this iteration of the loop.
        # (This happens when no files are found, in which case the
        # loop runs with target equal to the pattern itself.)
        _la_log 'Folder $_la_orig_install_dir seems to not contain .py files, skipping.'
        continue
    fi
    _la_symlink="$_la_symlink_dir/$(basename "$_la_target")"
    _la_log "   _la_target: $_la_target"
    _la_log "  _la_symlink: $_la_symlink"
    if [ -L "$_la_symlink" ] && [ "$(readlink "$_la_symlink")" = "$_la_target" ]; then
        _la_log 'symlink $_la_symlink already exists and points to $_la_target, skipping.'
        continue
    fi
    _la_log 'Creating symlink $_la_symlink pointing to $_la_target.'
    mkdir -p "$_la_symlink_dir" || true
    # this check also creates the symlink; if it fails, we enter the if-branch.
    if ! ln -sf "$_la_target" "$_la_symlink"; then
        echo -n "${BASH_SOURCE[0]} WARNING: Failed to create symlink from "
        echo "'$_la_target' to '$_la_symlink'!"
        echo "Unless you plan to use the GDB debugger with libarrow, this warning can be safely ignored."
        continue
    fi
done

_la_log "Libarrow activation complete."

unset _la_gdb_prefix
unset _la_log
unset _la_orig_install_dir
unset _la_placeholder
unset _la_symlink
unset _la_symlink_dir
unset _la_target

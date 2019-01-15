require 'mkmf-gnome2'

unless required_pkg_config_package("arrow")
  exit(false)
end

unless required_pkg_config_package("arrow-glib")
  exit(false)
end

[
  ["glib2", "ext/glib2"],
].each do |name, source_dir|
  spec = find_gem_spec(name)
  source_dir = File.join(spec.full_gem_path, source_dir)
  build_dir = source_dir
  add_depend_package_path(name, source_dir, build_dir)
end

$CXXFLAGS += ' -std=c++11 -Wno-deprecated-register'

create_makefile('arrow')

//  (C) Copyright John Maddock 2020

//  Use, modification and distribution are subject to the
//  Boost Software License, Version 1.0. (See accompanying file
//  LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

//  See http://www.boost.org/libs/config for more information.

//  MACRO:         BOOST_NO_CXX17_HDR_FILESYSTEM
//  TITLE:         C++17 header <filesystem> unavailable
//  DESCRIPTION:   The standard library does not supply C++17 header <filesystem>

#include <filesystem>

namespace boost_no_cxx17_hdr_filesystem {

int test()
{
  using std::filesystem::path;
  using std::filesystem::filesystem_error;
  using std::filesystem::directory_entry;
  using std::filesystem::directory_iterator;
  using std::filesystem::recursive_directory_iterator;
  using std::filesystem::file_status;
  using std::filesystem::space_info;
  using std::filesystem::file_type;
  using std::filesystem::perms;
  using std::filesystem::perm_options;
  using std::filesystem::copy_options;
  using std::filesystem::directory_options;
  using std::filesystem::absolute;
  using std::filesystem::canonical;
  using std::filesystem::copy;
  using std::filesystem::copy_file;
  using std::filesystem::copy_symlink;
  using std::filesystem::create_directories;
  using std::filesystem::create_directory;
  using std::filesystem::create_directory_symlink;
  using std::filesystem::create_hard_link;
  using std::filesystem::create_symlink;
  using std::filesystem::current_path;
  using std::filesystem::equivalent;
  using std::filesystem::exists;
  using std::filesystem::file_size;
  using std::filesystem::hard_link_count;
  using std::filesystem::is_block_file;
  using std::filesystem::is_character_file;
  using std::filesystem::is_directory;
  using std::filesystem::is_empty;
  using std::filesystem::is_fifo;
  using std::filesystem::is_other;
  using std::filesystem::is_regular_file;
  using std::filesystem::is_socket;
  using std::filesystem::is_symlink;
  using std::filesystem::last_write_time;
  using std::filesystem::permissions;
  using std::filesystem::proximate;
  using std::filesystem::read_symlink;
  using std::filesystem::relative;
  using std::filesystem::remove;
  using std::filesystem::remove_all;
  using std::filesystem::rename;
  using std::filesystem::resize_file;
  using std::filesystem::space;
  using std::filesystem::status;
  using std::filesystem::status_known;
  using std::filesystem::symlink_status;
  using std::filesystem::temp_directory_path;
  using std::filesystem::weakly_canonical;
  return 0;
}

}

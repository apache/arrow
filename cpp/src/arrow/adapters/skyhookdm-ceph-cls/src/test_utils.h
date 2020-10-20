#include <iostream>
#include <rados/librados.hpp>
#include <sstream>
#include <string>

using namespace librados;

// the below functions are taken from ceph src tree:
// https://github.com/ceph/ceph/blob/master/src/test/librados/test_shared.h

std::string get_temp_pool_name(const std::string &prefix = "test-rados-api-");
std::string create_one_pool_pp(const std::string &pool_name,
                               librados::Rados &cluster);
std::string
create_one_pool_pp(const std::string &pool_name, librados::Rados &cluster,
                   const std::map<std::string, std::string> &config);
std::string create_one_ec_pool_pp(const std::string &pool_name,
                                  librados::Rados &cluster);
std::string connect_cluster_pp(librados::Rados &cluster);
std::string
connect_cluster_pp(librados::Rados &cluster,
                   const std::map<std::string, std::string> &config);
int destroy_one_pool_pp(const std::string &pool_name, librados::Rados &cluster);

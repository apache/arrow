#include "test_utils.h"
using namespace librados;

std::string get_temp_pool_name(const std::string &prefix) {
  char hostname[80];
  char out[160];
  memset(hostname, 0, sizeof(hostname));
  memset(out, 0, sizeof(out));
  gethostname(hostname, sizeof(hostname) - 1);
  static int num = 1;
  snprintf(out, sizeof(out), "%s-%d-%d", hostname, getpid(), num);
  num++;
  return prefix + out;
}

std::string create_one_pool_pp(const std::string &pool_name, Rados &cluster) {
  return create_one_pool_pp(pool_name, cluster, {});
}

std::string
create_one_pool_pp(const std::string &pool_name, Rados &cluster,
                   const std::map<std::string, std::string> &config) {
  std::string err = connect_cluster_pp(cluster, config);
  if (err.length())
    return err;
  int ret = cluster.pool_create(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.pool_create(" << pool_name << ") failed with error " << ret;
    return oss.str();
  }

  IoCtx ioctx;
  ret = cluster.ioctx_create(pool_name.c_str(), ioctx);
  if (ret < 0) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.ioctx_create(" << pool_name << ") failed with error "
        << ret;
    return oss.str();
  }
  ioctx.application_enable("rados", true);
  return "";
}

int destroy_ruleset_pp(Rados &cluster, const std::string &ruleset,
                       std::ostream &oss) {
  bufferlist inbl;
  int ret = cluster.mon_command(
      "{\"prefix\": \"osd crush rule rm\", \"name\":\"" + ruleset + "\"}", inbl,
      NULL, NULL);
  if (ret)
    oss << "mon_command: osd crush rule rm " + ruleset + " failed with error "
        << ret << std::endl;
  return ret;
}

int destroy_ec_profile_pp(Rados &cluster, const std::string &pool_name,
                          std::ostream &oss) {
  bufferlist inbl;
  int ret = cluster.mon_command(
      "{\"prefix\": \"osd erasure-code-profile rm\", \"name\": \"testprofile-" +
          pool_name + "\"}",
      inbl, NULL, NULL);
  if (ret)
    oss << "mon_command: osd erasure-code-profile rm testprofile-" << pool_name
        << " failed with error " << ret << std::endl;
  return ret;
}

int destroy_ec_profile_and_ruleset_pp(Rados &cluster,
                                      const std::string &ruleset,
                                      std::ostream &oss) {
  int ret;
  ret = destroy_ec_profile_pp(cluster, ruleset, oss);
  if (ret)
    return ret;
  return destroy_ruleset_pp(cluster, ruleset, oss);
}

std::string create_one_ec_pool_pp(const std::string &pool_name,
                                  Rados &cluster) {
  std::string err = connect_cluster_pp(cluster);
  if (err.length())
    return err;

  std::ostringstream oss;
  int ret = destroy_ec_profile_and_ruleset_pp(cluster, pool_name, oss);
  if (ret) {
    cluster.shutdown();
    return oss.str();
  }

  bufferlist inbl;
  ret = cluster.mon_command(
      "{\"prefix\": \"osd erasure-code-profile set\", \"name\": "
      "\"testprofile-" +
          pool_name +
          "\", \"profile\": [ \"k=2\", \"m=1\", \"crush-failure-domain=osd\"]}",
      inbl, NULL, NULL);
  if (ret) {
    cluster.shutdown();
    oss << "mon_command erasure-code-profile set name:testprofile-" << pool_name
        << " failed with error " << ret;
    return oss.str();
  }

  ret = cluster.mon_command(
      "{\"prefix\": \"osd pool create\", \"pool\": \"" + pool_name +
          "\", \"pool_type\":\"erasure\", \"pg_num\":8, \"pgp_num\":8, "
          "\"erasure_code_profile\":\"testprofile-" +
          pool_name + "\"}",
      inbl, NULL, NULL);
  if (ret) {
    bufferlist inbl;
    destroy_ec_profile_pp(cluster, pool_name, oss);
    cluster.shutdown();
    oss << "mon_command osd pool create pool:" << pool_name
        << " pool_type:erasure failed with error " << ret;
    return oss.str();
  }

  cluster.wait_for_latest_osdmap();
  return "";
}

std::string connect_cluster_pp(librados::Rados &cluster) {
  return connect_cluster_pp(cluster, {});
}

std::string
connect_cluster_pp(librados::Rados &cluster,
                   const std::map<std::string, std::string> &config) {
  char *id = getenv("CEPH_CLIENT_ID");
  if (id)
    std::cerr << "Client id is: " << id << std::endl;

  int ret;
  ret = cluster.init(id);
  if (ret) {
    std::ostringstream oss;
    oss << "cluster.init failed with error " << ret;
    return oss.str();
  }
  ret = cluster.conf_read_file(NULL);
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.conf_read_file failed with error " << ret;
    return oss.str();
  }
  cluster.conf_parse_env(NULL);

  for (auto &setting : config) {
    ret = cluster.conf_set(setting.first.c_str(), setting.second.c_str());
    if (ret) {
      std::ostringstream oss;
      oss << "failed to set config value " << setting.first << " to '"
          << setting.second << "': " << strerror(-ret);
      return oss.str();
    }
  }

  ret = cluster.connect();
  if (ret) {
    cluster.shutdown();
    std::ostringstream oss;
    oss << "cluster.connect failed with error " << ret;
    return oss.str();
  }
  return "";
}

int destroy_one_pool_pp(const std::string &pool_name, Rados &cluster) {
  int ret = cluster.pool_delete(pool_name.c_str());
  if (ret) {
    cluster.shutdown();
    return ret;
  }
  cluster.shutdown();
  return 0;
}

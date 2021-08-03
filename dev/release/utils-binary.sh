# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

docker_image_name=apache-arrow/release-binary
gpg_agent_extra_socket="$(gpgconf --list-dirs agent-extra-socket)"
if [ $(uname) = "Darwin" ]; then
  docker_uid=10000
  docker_gid=10000
else
  docker_uid=$(id -u)
  docker_gid=$(id -g)
fi
docker_ssh_key="${SOURCE_DIR}/binary/id_rsa"

if [ ! -f "${docker_ssh_key}" ]; then
  ssh-keygen -N "" -f "${docker_ssh_key}"
fi

docker_gpg_ssh() {
  local ssh_port=$1
  shift
  local known_hosts_file=$(mktemp -t "arrow-binary-gpg-ssh-known-hosts.XXXXX")
  local exit_code=
  if ssh \
      -o StrictHostKeyChecking=no \
      -o UserKnownHostsFile=${known_hosts_file} \
      -i "${docker_ssh_key}" \
      -p ${ssh_port} \
      -R "/home/arrow/.gnupg/S.gpg-agent:${gpg_agent_extra_socket}" \
      arrow@127.0.0.1 \
      "$@"; then
    exit_code=$?;
  else
    exit_code=$?;
  fi
  rm -f ${known_hosts_file}
  return ${exit_code}
}

docker_run() {
  local container_id_dir=$(mktemp -d -t "arrow-binary-gpg-container.XXXXX")
  local container_id_file=${container_id_dir}/id
  docker \
    run \
    --cidfile ${container_id_file} \
    --detach \
    --publish-all \
    --rm \
    --volume "$PWD":/host \
    ${docker_image_name} \
    bash -c "
if [ \$(id -u) -ne ${docker_uid} ]; then
  usermod --uid ${docker_uid} arrow
  chown -R arrow: ~arrow
fi
/usr/sbin/sshd -D
"
  local container_id=$(cat ${container_id_file})
  local ssh_port=$(docker port ${container_id} | grep -E -o '[0-9]+$' | head -n 1)
  # Wait for sshd available
  while ! docker_gpg_ssh ${ssh_port} : > /dev/null 2>&1; do
    sleep 0.1
  done
  gpg --export ${GPG_KEY_ID} | docker_gpg_ssh ${ssh_port} gpg --import
  docker_gpg_ssh ${ssh_port} "$@"
  docker kill ${container_id}
  rm -rf ${container_id_dir}
}

docker build -t ${docker_image_name} "${SOURCE_DIR}/binary"

chmod go-rwx "${docker_ssh_key}"

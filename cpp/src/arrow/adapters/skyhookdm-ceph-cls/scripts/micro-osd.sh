set -e
set -u
set -x

TEST_DIR=$1
CONF_DIR=$2
echo "TEST_DIR=${TEST_DIR}"
echo "CONF_DIR=${CONF_DIR}"
echo "pwd="`pwd`

# get rid of process and directories leftovers
pkill ceph-mon || true
pkill ceph-osd || true
rm -fr $TEST_DIR

# cluster wide parameters
mkdir -p ${TEST_DIR}/log
cat >> $CONF_DIR/ceph.conf <<EOF
[global]
fsid = $(uuidgen)
osd crush chooseleaf type = 0
run dir = ${TEST_DIR}/run
auth cluster required = none
auth service required = none
auth client required = none
osd pool default size = 1
EOF
export CEPH_ARGS="--conf ${CONF_DIR}/ceph.conf"

# single monitor
MON_DATA=${TEST_DIR}/mon
mkdir -p $MON_DATA

cat >> $CONF_DIR/ceph.conf <<EOF
[mon.0]
log file = ${TEST_DIR}/log/mon.log
chdir = ""
mon cluster log file = ${TEST_DIR}/log/mon-cluster.log
mon data = ${MON_DATA}
mon addr = 127.0.0.1
# this was added to enable pool deletion within method delete_one_pool_pp()
mon_allow_pool_delete = true
EOF

ceph-mon --id 0 --mkfs --keyring /dev/null
touch ${MON_DATA}/keyring
ceph-mon --id 0

# single osd
OSD_DATA=${TEST_DIR}/osd
mkdir ${OSD_DATA}

cat >> $CONF_DIR/ceph.conf <<EOF
[osd.0]
log file = ${TEST_DIR}/log/osd.log
chdir = ""
osd data = ${OSD_DATA}
osd journal = ${OSD_DATA}.journal
osd journal size = 100
osd objectstore = memstore
osd class load list = lock log numops refcount replica_log statelog timeindex user version arrow
EOF

OSD_ID=$(ceph osd create)
ceph osd crush add osd.${OSD_ID} 1 root=default host=localhost
ceph-osd --id ${OSD_ID} --mkjournal --mkfs
ceph-osd --id ${OSD_ID}

# check that it works
ceph osd pool create rbd 8 8
rados --pool rbd put group /etc/group
rados --pool rbd get group ${TEST_DIR}/group
diff /etc/group ${TEST_DIR}/group
ceph osd tree

export CEPH_CONF="${CONF_DIR}/ceph.conf"

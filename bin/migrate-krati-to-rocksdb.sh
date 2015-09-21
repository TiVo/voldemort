#!/bin/bash -e

BIN_DIR=$(cd $(dirname $0) && pwd)
HOME_DIR=$1

if [ $# -ne 1 ];
then
    echo "USAGE: bin/migrate-krati-to-rocksdb.sh [voldemort_home]"
    exit 1
fi

# Ensure script is run as 'ffs' so that the data diretories are created with the correct owner/permissions.
if [ $(whoami) != "ffs" ]; then
    /bin/echo "This script should be run as the 'ffs' user."
    exit 1
fi

# Be sure Voldemort is not currently running.
pids=`ps xwww | grep voldemort.server.VoldemortServe[r] | awk '{print $1}'`
if [ "$pids" != "" ]
then
    echo "Voldemort is running, please shut it down before attempting migration."
    exit 1
fi

# Ensure the Voldemort config files exist.
if [ ! -d ${HOME_DIR}/config/STORES ] || [ ! -f ${HOME_DIR}/config/server.properties ]; then
    echo "Could not finde Voldemort config files. Can not migrate."
    exit 1
fi

# Ensure the Krati data directory exists.
if [ ! -d ${HOME_DIR}/data/krati ]; then
    echo "Could not find the Krati data dir. Can not migrate."
    exit 1
fi

# Backup the Voldemort config files we are about to modify.
if [ ! -d ${HOME_DIR}/config/STORES.krati ]; then
    cp -R ${HOME_DIR}/config/STORES ${HOME_DIR}/config/STORES.krati
fi
if [ ! -f ${HOME_DIR}/config/server.properties.krati ]; then
    cp -R ${HOME_DIR}/config/server.properties ${HOME_DIR}/config/server.properties.krati
fi

# Perform the migration.
${BIN_DIR}/run-class.sh voldemort.tools.MigrateKratiToRocksDB $@

# Update the config files.
sed -i 's|<persistence>krati</persistence>|<persistence>rocksdb</persistence>|' ${HOME_DIR}/config/STORES/*
sed -i 's|slop.store.engine=krati|slop.store.engine=rocksdb|' ${HOME_DIR}/config/server.properties

# Ensure the old stores.xml file is empty.
echo -e "<stores>\n</stores>" > ${HOME_DIR}/config/stores.xml

# Move the old Krati data out of the way just to make sure it does not get used.
mkdir -p ${HOME_DIR}/data/krati.bak
mv ${HOME_DIR}/data/krati/* ${HOME_DIR}/data/krati.bak

echo
echo "*************************************************************************************************"
echo "* Migration complete.  Voldemort needs to be restarted. A 'restore-from-replica' is recommended."
if [ ! -f ${HOME_DIR}/data/restored ]; then
    echo "*   The restore marker file does not exist: ${HOME_DIR}/data/restored"
    echo "*   A 'restore-from-replica' may be automatically performed when Voldemort is restarted."
fi
echo "* The old Krati data is backed up at the following location: ${HOME_DIR}/data/krati.bak"
echo "*************************************************************************************************"


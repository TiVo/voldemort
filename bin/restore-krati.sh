#!/bin/bash -e

HOME_DIR=$1

if [ $# -ne 1 ];
then
    echo 'USAGE: bin/restore-krati.sh [voldemort_home]'
    exit 1
fi

# Ensure Voldemort is not currently running.
pids=`ps xwww | grep voldemort.server.VoldemortServe[r] | awk '{print $1}'`
if [ "$pids" != "" ]
then
    echo 'Voldemort is running, please shut it down before attempting to restore the Krati data.'
    exit 1
fi

# Ensure the Krati config file backups exist.
if [ ! -d ${HOME_DIR}/config/STORES.krati ] || [ ! -f ${HOME_DIR}/config/server.properties.krati ]; then
    echo 'Missing Krati config file backups. Can not restore.'
    exit 1
fi

# Ensure the Krati data file backups exist.
if [ ! -d ${HOME_DIR}/data/krati.bak ]; then
    echo 'Missing Krati data file backups. Can not restore.'
    exit 1
fi

# Restore the conifg files to their original state.
rm -rf ${HOME_DIR}/config/STORES
mv ${HOME_DIR}/config/STORES.krati ${HOME_DIR}/config/STORES
mv ${HOME_DIR}/config/server.properties.krati ${HOME_DIR}/config/server.properties

# Remove the old data dirs.
rm -rf ${HOME_DIR}/data/rocksdb
rm -rf ${HOME_DIR}/data/krati

# Move the old Krati data direcotry into place.
mv ${HOME_DIR}/data/krati.bak ${HOME_DIR}/data/krati

#!/bin/bash

HOSTS="$1"
PATHE="$2"
NODE_CNT="$3"
USERNAME="$4"
LDLIB="$7"
count=0

for HOSTNAME in ${HOSTS}; do
    #SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 10m 10m gdb -batch -ex \"run\" -ex \"bt\" --args ./rundb -nid${count} >> results.out 2>&1 | grep -v ^\"No stack.\"$"
    if [ $count -ge $NODE_CNT ]; then
        SCRIPT="ulimit -c unlimited; export LD_LIBRARY_PATH=${LDLIB}:$LD_LIBRARY_PATH; env SCHEMA_PATH=\"$2\" timeout -k 9 10m ${PATHE}runcl -nid${count} > ${PATHE}clresults${count}.out 2>&1"
        echo "${HOSTNAME}: runcl ${count}"
    else
        # SCRIPT="env SCHEMA_PATH=\"$2\" timeout -k 9 10m ${PATHE}rundb -nid${count} > ${PATHE}dbresults.out 2>&1"
        SCRIPT="ulimit -c unlimited; export LD_LIBRARY_PATH=${LDLIB}:$LD_LIBRARY_PATH; env SCHEMA_PATH=\"$2\" ${PATHE}rundb -nid${count} > ${PATHE}dbresults${count}.out 2>&1"
        echo "${HOSTNAME}: rundb ${count}"
    fi
    ssh -n -o BatchMode=yes -o StrictHostKeyChecking=no ${USERNAME}@${HOSTNAME} "${SCRIPT}" &
    count=`expr $count + 1`
done

if [ $5 -ne 0 ]
then
    sleep 60
    OLD_IFS="$IFS"
    IFS=" "
    HOSTLIST=($HOSTS)
    IFS="$OLD_IFS"
    scp run_perf.sh ${USERNAME}@${HOSTLIST[0]}:${PATHE}
    ssh ${USERNAME}@${HOSTLIST[0]} "bash ${PATHE}/run_perf.sh $5 $6"
fi

while [ $count -gt 0 ]
do
    wait $pids
    count=`expr $count - 1`
done

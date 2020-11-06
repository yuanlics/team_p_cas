#!/usr/bin/env bash

# bash run_node.sh /temp/team_p/project-files/xact-files/ <node_id> <n_clients> <True/False> <QUORUM/ONE>
# E.g.
# bash run_node.sh /temp/team_p/project-files/xact-files/ 1 10 False QUORUM

xact_dir=$1
node_id=$2
n_clients=$3
print=$4
level=$5
local_ip=$6

mkdir -p ~/log

for client_id in $(seq 1 $n_clients)
do
    if [ $((client_id%5 + 1)) == $node_id ]
    then
        report_file=~/log/${n_clients}_${level}_client${client_id}.csv
        python3 driver.py --xact-dir $xact_dir --client-id $client_id --print $print --report-file $report_file --level $level --ip $local_ip &
        echo Node $node_id initiated Client $client_id
    fi
done

echo Waiting clients

wait

echo Clients finished

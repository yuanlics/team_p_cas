#!/usr/bin/env bash

# python3 load.py --data-dir /temp/team_p/project-files/data-files/ --cql-dir /temp/team_p/apache-cassandra-3.11.6/bin/cqlsh --ip 192.168.48.184

export local_ip=192.168.48.184
export ips=(192.168.48.184 192.168.48.185 192.168.48.186 192.168.48.187 192.168.48.188)
export print_terminal=false
export data_dir=/temp/team_p/project-files/data-files/
export xact_dir=/temp/team_p/project-files/xact-files/
export cassandra_root=/temp/team_p/apache-cassandra-3.11.6/
export cql_dir=/temp/team_p/apache-cassandra-3.11.6/bin/cqlsh
export project_dir=/temp/team_p/team_p_cas/

for n_clients in 20
do
    for level in QUORUM
    do
        echo "Start experiment $n_clients $level"
        ssh xcnc35 "cd $project_dir && git pull && python3 load.py --data-dir $data_dir --cql-dir $cql_dir --ip $local_ip"
        echo "Reloaded DB"
        ssh xcnc35 "cd $project_dir && git pull && bash run_node.sh $xact_dir 1 $n_clients $print_terminal $level ${ips[0]}" &
        ssh xcnc36 "cd $project_dir && git pull && bash run_node.sh $xact_dir 2 $n_clients $print_terminal $level ${ips[1]}" &
        ssh xcnc37 "cd $project_dir && git pull && bash run_node.sh $xact_dir 3 $n_clients $print_terminal $level ${ips[2]}" &
        ssh xcnc38 "cd $project_dir && git pull && bash run_node.sh $xact_dir 4 $n_clients $print_terminal $level ${ips[3]}" &
        ssh xcnc39 "cd $project_dir && git pull && bash run_node.sh $xact_dir 5 $n_clients $print_terminal $level ${ips[4]}" &
        wait
        echo "All nodes finished"
        ssh xcnc35 "cd $project_dir && python3 summary.py --nc $n_clients --level $level --ip $local_ip"
        echo "Finished experiment $n_clients $level"
    done

done
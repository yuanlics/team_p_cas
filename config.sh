#!/usr/bin/env bash

# E.g. bash config.sh <local_ip> "192.168.48.184, 192.168.48.185, 192.168.48.186" /temp/team_p/apache-cassandra-3.11.6/

local_ip=$1
seed_ips=$2
cassandra_dir=$3

pushd $cassandra_dir

sed -i "s/^cluster_name: 'Test Cluster'\$/cluster_name: 'ClusterP'/g" conf/cassandra.yaml
sed -i "s/seeds: \"127.0.0.1:7000\"\$/seeds: \"192.168.48.184, 192.168.48.185, 192.168.48.186, 192.168.48.187, 192.168.48.188\"/g" conf/cassandra.yaml
sed -i "s/^listen_address: localhost\$/listen_address: 192.168.48.184/g" conf/cassandra.yaml
sed -i "s/^rpc_address: localhost\$/rpc_address: 192.168.48.184/g" conf/cassandra.yaml
sed -i "s/^endpoint_snitch: SimpleSnitch$/endpoint_snitch: GossipingPropertyFileSnitch/g" conf/cassandra.yaml
sed -i "s/request_timeout_in_ms:.*\$/request_timeout_in_ms: 1000000/" conf/cassandra.yaml

./bin/cassandra

popd

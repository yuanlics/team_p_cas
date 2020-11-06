# team_p_cas

## Requirements

Make sure `python3, python-pandas, python-cassandra-driver` are installed.
    
## Configure Cassandra

On each node, enter the project directory and run:

    bash config.sh <local_ip> <seed_ips> <cassandra_root>
    
For our example, run the following lines on each node:

    bash config.sh 192.168.48.184 "192.168.48.184, 192.168.48.185, 192.168.48.186, 192.168.48.187, 192.168.48.188" /temp/team_p/apache-cassandra-3.11.6/
    bash config.sh 192.168.48.185 "192.168.48.184, 192.168.48.185, 192.168.48.186, 192.168.48.187, 192.168.48.188" /temp/team_p/apache-cassandra-3.11.6/
    bash config.sh 192.168.48.186 "192.168.48.184, 192.168.48.185, 192.168.48.186, 192.168.48.187, 192.168.48.188" /temp/team_p/apache-cassandra-3.11.6/
    bash config.sh 192.168.48.187 "192.168.48.184, 192.168.48.185, 192.168.48.186, 192.168.48.187, 192.168.48.188" /temp/team_p/apache-cassandra-3.11.6/
    bash config.sh 192.168.48.188 "192.168.48.184, 192.168.48.185, 192.168.48.186, 192.168.48.187, 192.168.48.188" /temp/team_p/apache-cassandra-3.11.6/
   
## Run Experiment

On node 1, edit the configurations in `run_exp.sh`, then run the following line to run all four experiments.

    bash run_exp.sh
    
The corresponding csv results are saved in `~/log/`
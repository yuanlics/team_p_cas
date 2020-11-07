import csv

log_dir = '/home/stuproj/cs4224p/log/'

ncs = ['20', '40']
levels = ['QUORUM', 'ONE']
exp_cnt = 1
clients = []
for nc in ncs:
    for level in levels:
        for i in range(int(nc)):
            with open(log_dir+nc+'_'+level+'_client'+str(i+1)+'.csv', 'r') as f:
                csv_reader = csv.reader(f)
                for row in csv_reader:
                    temp = [str(exp_cnt)] + row
                    clients.append(temp)
        exp_cnt += 1

with open('log/' + 'cas_clients.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['exp_num', 'client_num', 'xact_num', 'exec_time', 'throughput',
                     'avg_latency', 'median_latency', '95th_latency', '99th_latency'])
    for row in clients:
        writer.writerow(row)


exp_cnt = 1
dbstates = []
for nc in ncs:
    for level in levels:
        with open('log/' + nc+'_'+level+'_dbstate.csv', 'r') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                temp = [str(exp_cnt)] + row
                dbstates.append(temp)
        exp_cnt += 1
        
with open('log/' + 'cas_dbstate.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['exp_num', 'sum_w_ytd', 'sum_d_ytd', 'sum_d_next_o_id', 'sum_c_balance',
                     'sum_c_ytd_payment', 'sum_c_payment_cnt', 'sum_c_delivery_cnt', 
                     'max_o_id', 'sum_o_ol_cnt', 'sum_ol_amount', 'sum_ol_quantity',
                     'sum_s_quantity', 'sum_s_ytd', 'sum_s_order_cnt', 'sum_s_remote_cnt'])
    for row in dbstates:
        writer.writerow(row)

        
exp_cnt = 1
throughputs = []
for nc in ncs:
    for level in levels:
        with open('log/' + nc+'_'+level+'_throughput.csv', 'r') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                temp = [str(exp_cnt)] + row
                throughputs.append(temp)
        exp_cnt += 1
        
with open('log/' + 'cas_throughput.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['exp_num', 'min_throughput', 'avg_throughput', 'max_throughput'])
    for row in throughputs:
        writer.writerow(row)
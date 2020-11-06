# db state
import csv
import numpy as np
import argparse
from cassandra.cluster import Cluster

parser = argparse.ArgumentParser(description='Summary')
parser.add_argument('--nc', default='', type=str, help='Number of client')
parser.add_argument('--level', default='', type=str, help='Consistency level')
parser.add_argument('--ip', default='127.0.0.1', type=str, help='Local ip')
parser.add_argument('--log_dir', default='/home/stuproj/cs4224p/log/', type=str, help='log dir')
args = parser.parse_args()

nc = args.nc
level = args.level
ip = args.ip

cluster = Cluster(contact_points=[ip]*20, connect_timeout=100)
sess = cluster.connect('wholesale')
sess.default_timeout = 3000.0
print(sess.default_timeout)

res = []
rows = sess.execute("SELECT sum(w_ytd) FROM warehouse", timeout=3000)
res.append(float(rows.one().system_sum_w_ytd))

rows = sess.execute("SELECT sum(d_ytd), sum(d_next_o_id) FROM district")
res.append(float(rows.one().system_sum_d_ytd))
res.append(rows.one().system_sum_d_next_o_id)

rows = sess.execute("SELECT sum(c_balance), sum(c_ytd_payment), sum(c_payment_cnt), sum(c_delivery_cnt) FROM customer")
res.append(float(rows.one().system_sum_c_balance))
res.append(float(rows.one().system_sum_c_ytd_payment))
res.append(rows.one().system_sum_c_payment_cnt)
res.append(rows.one().system_sum_c_delivery_cnt)

rows = sess.execute("SELECT max(o_id), sum(o_ol_cnt) FROM orders")
res.append(rows.one().system_max_o_id)
res.append(float(rows.one().system_sum_o_ol_cnt))

rows = sess.execute("SELECT sum(ol_amount), sum(ol_quantity) FROM order_line")
res.append(float(rows.one().system_sum_ol_amount))
res.append(float(rows.one().system_sum_ol_quantity))

rows = sess.execute("SELECT sum(s_quantity), sum(s_ytd), sum(s_order_cnt), sum(s_remote_cnt) FROM stock")
res.append(float(rows.one().system_sum_s_quantity))
res.append(float(rows.one().system_sum_s_ytd))
res.append(rows.one().system_sum_s_order_cnt)
res.append(rows.one().system_sum_s_remote_cnt)

print('db state:', res)

with open('log/' + nc+'_'+level+'_dbstate.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(res)

throughputs = []
for i in range(int(nc)):
    with open(args.log_dir + nc+'_'+level+'_client'+str(i+1)+'.csv', 'r') as f:
        csv_reader = csv.reader(f)
        for row in csv_reader:
            throughputs.append(float(row[3]))

with open('log/' + nc+'_'+level+'_throughput.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow([np.min(throughputs), round(np.mean(throughputs),2), np.max(throughputs)])
import sys
import csv
import time
import argparse
import traceback
import numpy as np

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.cluster import ExecutionProfile

from new_order import NewOrder
from payment import Payment
from delivery import Delivery
from order_status import OrderStatus
from stock_level import StockLevel
from popular_item import PopularItem
from top_balance import TopBalance
from related_customer import RelatedCustomer

parser = argparse.ArgumentParser(description='Driver')
parser.add_argument('--xact-dir', default='', type=str, help='Xact directory')
parser.add_argument('--client-id', default='', type=str, help='Client id')
parser.add_argument('--print', default='false', help='Print to screen or not')
parser.add_argument('--report-file', default='', type=str, help='Report file name')
parser.add_argument('--level', default='QUORUM', type=str, help='Consistency level')
parser.add_argument('--ip', default='127.0.0.1', type=str, help='Local ip')
args = parser.parse_args()
args.print = True if args.print.lower() == 'true' else False

xact_dir = args.xact_dir
client_id = args.client_id
consist_level = args.level
ip = args.ip

cluster = Cluster(contact_points=[ip]*20, connect_timeout=100)
if consist_level == 'ONE':
    profile1 = ExecutionProfile(consistency_level=ConsistencyLevel.ONE, request_timeout=120.0)
    cluster.add_execution_profile('one', profile1)
    profile2 = ExecutionProfile(consistency_level=ConsistencyLevel.ALL, request_timeout=120.0)
    cluster.add_execution_profile('all', profile2)
elif consist_level == 'QUORUM':
    profile = ExecutionProfile(consistency_level=ConsistencyLevel.QUORUM, request_timeout=120.0)
    cluster.add_execution_profile('quorum', profile)

sess = cluster.connect('wholesale')
no = NewOrder(sess, consist_level)
pa = Payment(sess, consist_level)
de = Delivery(sess, consist_level)
os = OrderStatus(sess, consist_level)
sl = StockLevel(sess, consist_level)
pi = PopularItem(sess, consist_level)
tb = TopBalance(sess, consist_level)
rc = RelatedCustomer(sess, consist_level)

with open(xact_dir+'/'+client_id+'.txt') as f:
    lines = f.readlines()
    xact_cnt = 0
    row_cnt = 0
    latency = []
    t1 = time.time()
    while row_cnt < len(lines):
        inputs = lines[row_cnt].strip('\n').split(',')
        row_cnt += 1
        category = inputs[0]
        try:
            t3 = time.time()
            if category == 'N':
                i_id = []
                w_id = []
                quantity = []
                num_items = int(inputs[4])
                tmp = row_cnt
                while row_cnt < tmp+num_items:
                    item = lines[row_cnt].strip('\n').split(',')
                    row_cnt += 1
                    i_id.append(int(item[0]))
                    w_id.append(int(item[1]))
                    quantity.append(int(item[2]))
                res = no.exec_xact(int(inputs[1]), int(inputs[2]), int(inputs[3]), num_items, i_id, w_id, quantity)
            elif category == 'P':
                res = pa.exec_xact(int(inputs[1]), int(inputs[2]), int(inputs[3]), float(inputs[4]))
            elif category == 'D':
                res = de.exec_xact(int(inputs[1]), int(inputs[2]))
            elif category == 'O':
                res = os.exec_xact(int(inputs[1]), int(inputs[2]), int(inputs[3]))
            elif category == 'S':
                res = sl.exec_xact(int(inputs[1]), int(inputs[2]), int(inputs[3]), int(inputs[4]))
            elif category == 'I':
                res = pi.exec_xact(int(inputs[1]), int(inputs[2]), int(inputs[3]))
            elif category == 'T':
                res = tb.exec_xact()         
            elif category == 'R':
                res = rc.exec_xact(int(inputs[1]), int(inputs[2]), int(inputs[3]))

            t4 = time.time()
            latency.append(round((t4-t3)*1000,2))
            xact_cnt += 1
#             print('Client', client_id, 'Current Xact:', xact_cnt)

            if args.print:
                sys.stdout.write('Xact: {0}\n{1}\n'.format(xact_cnt, res))
        except Exception as e:
            with open('err.txt','a') as f:
                f.write(client_id+': '+str(inputs)+'\n'+traceback.format_exc()+'\n')
            continue

print(client_id, xact_cnt)
t2 = time.time()
exec_time = round(t2-t1,2)
throughput = round(xact_cnt/exec_time,2)

l_mean = round(np.mean(latency),2)
l_median = round(np.median(latency),2)
l_per95 = round(np.percentile(latency, 95),2)
l_per99 = round(np.percentile(latency, 99),2)

if args.print:
    sys.stderr.write(f'Client {client_id}:\nXact Num {xact_cnt}; Exec Time {exec_time}; ' +
                     f'Throughput {throughput}; Average Latency {l_mean}; Median Latency {l_median}; ' +
                     f'95th Percentile Latency {l_per95}; 99th Percentile Latency {l_per99}\n')

with open(args.report_file, 'w') as f:
    writer = csv.writer(f)
#     writer.writerow(['client_number', 'measurement_a', 'measurement_b', 'measurement_c', 'measurement_d',
#                      'measurement_e', 'measurement_f', 'measurement_g'])
    writer.writerow([client_id, xact_cnt, exec_time, throughput, l_mean, l_median, l_per95, l_per99])
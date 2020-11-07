# import os
# import csv
# import argparse
# import pandas as pd
# from cassandra.cluster import Cluster
# import time

# parser = argparse.ArgumentParser(description='Load')
# parser.add_argument('--data-dir', default='/home/liyuan/data1/distdb/project-files/data-files', type=str, help='Data directory')
# parser.add_argument('--cql-dir', default='/home/liyuan/data1/distdb/apache-cassandra-3.11.6/bin/cqlsh', type=str, help='CQL directory')
# parser.add_argument('--ip', default='127.0.0.1', type=str, help='Local ip')
# args = parser.parse_args()

# DATA_DIR = args.data_dir
# CQL_DIR = args.cql_dir
# KEYSPACE = 'wholesale'
# ip = args.ip
# REP_STRATEGY = 'SimpleStrategy'
# REP_FACTOR = '3'

# ### create table queries ###
# create_warehouse_q = \
# '''CREATE TABLE warehouse (
#     w_id int,
#     w_name text,
#     w_street_1 text,
#     w_street_2 text,
#     w_city text,
#     w_state text,
#     w_zip text,
#     w_tax decimal,
#     w_ytd decimal,
#     PRIMARY KEY (w_id)
# )'''

# # add d_next_deliver_o_id
# create_district_q = \
# '''CREATE TABLE district (
#     d_w_id int,
#     d_id int,
#     d_name text,
#     d_street_1 text,
#     d_street_2 text,
#     d_city text,
#     d_state text,
#     d_zip text,
#     d_tax decimal,
#     d_ytd decimal,
#     d_next_o_id int,
#     d_next_deliver_o_id int,
#     PRIMARY KEY (d_w_id, d_id)
# )'''

# # add c_w_name, c_d_name
# create_customer_q = \
# '''CREATE TABLE customer (
#     c_w_id int,
#     c_d_id int,
#     c_id int,
#     c_first text,
#     c_middle text,
#     c_last text,
#     c_street_1 text,
#     c_street_2 text,
#     c_city text,
#     c_state text,
#     c_zip text,
#     c_phone text,
#     c_since timestamp,
#     c_credit text,
#     c_credit_lim decimal,
#     c_discount decimal,
#     c_balance decimal,
#     c_ytd_payment float,
#     c_payment_cnt int,
#     c_delivery_cnt int,
#     c_data text,
#     c_w_name text,
#     c_d_name text,
#     PRIMARY KEY (c_w_id, c_d_id, c_id)
# )'''

# create_orders_q = \
# '''CREATE TABLE orders (
#     o_w_id int,
#     o_d_id int,
#     o_id int,
#     o_c_id int,
#     o_carrier_id int,
#     o_ol_cnt decimal,
#     o_all_local decimal,
#     o_entry_d timestamp,
#     PRIMARY KEY (o_w_id, o_d_id, o_id)
# ) WITH CLUSTERING ORDER BY (o_d_id DESC, o_id DESC)'''

# create_item_q = \
# '''CREATE TABLE item (
#     i_id int,
#     i_name text,
#     i_price decimal,
#     i_im_id int,
#     i_data text,
#     PRIMARY KEY (i_id)
# )'''

# # add ol_i_name
# create_order_line_q = \
# '''CREATE TABLE order_line (
#     ol_w_id int,
#     ol_d_id int,
#     ol_o_id int,
#     ol_number int,
#     ol_i_id int,
#     ol_delivery_d timestamp,
#     ol_amount decimal,
#     ol_supply_w_id int,
#     ol_quantity decimal,
#     ol_dist_info text,
#     ol_i_name text,
#     PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_quantity, ol_number, ol_i_id)
# ) WITH CLUSTERING ORDER BY (ol_d_id DESC, ol_o_id DESC, ol_quantity DESC)'''

# # add s_i_name, s_i_price
# create_stock_q = \
# '''CREATE TABLE stock (
#     s_w_id int,
#     s_i_id int,
#     s_quantity decimal,
#     s_ytd decimal,
#     s_order_cnt int,
#     s_remote_cnt int,
#     s_dist_01 text,
#     s_dist_02 text,
#     s_dist_03 text,
#     s_dist_04 text,
#     s_dist_05 text,
#     s_dist_06 text,
#     s_dist_07 text,
#     s_dist_08 text,
#     s_dist_09 text,
#     s_dist_10 text,
#     s_data text,
#     s_i_name text,
#     s_i_price decimal,
#     PRIMARY KEY (s_w_id, s_i_id)
# )'''

# create_view_customer_top_balance_q = \
# '''CREATE MATERIALIZED VIEW customer_sort_by_balance AS
#     SELECT c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_balance, c_w_name, c_d_name
#     FROM customer
#     WHERE c_w_id IS NOT NULL AND c_d_id IS NOT NULL
#         AND c_id IS NOT NULL AND c_balance IS NOT NULL
#     PRIMARY KEY (c_w_id, c_balance, c_d_id, c_id)
#     WITH CLUSTERING ORDER BY (c_balance DESC)'''

# create_view_order_by_customer_q = \
# '''CREATE MATERIALIZED VIEW order_sort_by_customer AS
#     SELECT o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_entry_d
#     FROM orders
#     WHERE o_w_id IS NOT NULL AND o_d_id IS NOT NULL 
#         AND o_id IS NOT NULL AND o_c_id IS NOT NULL
#     PRIMARY KEY (o_w_id, o_d_id, o_c_id, o_id)
#     WITH CLUSTERING ORDER BY (o_d_id DESC, o_c_id DESC, o_id DESC)'''

# create_order_line_by_item_q = \
# '''CREATE MATERIALIZED VIEW order_line_sort_by_item AS
#     SELECT ol_w_id, ol_d_id, ol_o_id, ol_quantity, ol_number, ol_i_id
#     FROM order_line
#     WHERE ol_w_id IS NOT NULL AND ol_d_id IS NOT NULL 
#       AND ol_o_id IS NOT NULL AND ol_quantity IS NOT NULL
#       AND ol_number IS NOT NULL AND ol_i_id IS NOT NULL
#     PRIMARY KEY (ol_w_id, ol_d_id, ol_i_id, ol_o_id, ol_quantity, ol_number)'''


# ### load data queries ###
# load_warehouse_q = \
# f'''COPY {KEYSPACE}.warehouse
# (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) 
# FROM '{DATA_DIR}/warehouse.csv' WITH DELIMITER=',' '''

# load_district_q = \
# f'''COPY {KEYSPACE}.district 
# (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id, d_next_deliver_o_id) 
# FROM '{DATA_DIR}/district-aug.csv' WITH DELIMITER=',' '''

# load_customer_q = \
# f'''COPY {KEYSPACE}.customer 
# (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, 
# c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, 
# c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data, c_w_name, c_d_name) 
# FROM '{DATA_DIR}/customer-aug.csv' WITH DELIMITER=',' '''

# load_orders_q = \
# f'''COPY {KEYSPACE}.orders 
# (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id, o_ol_cnt, o_all_local, o_entry_d) 
# FROM '{DATA_DIR}/order.csv' WITH DELIMITER=',' '''

# load_item_q = \
# f'''COPY {KEYSPACE}.item 
# (i_id, i_name, i_price, i_im_id, i_data) 
# FROM '{DATA_DIR}/item.csv' WITH DELIMITER=',' '''

# load_order_line_q = \
# f'''COPY {KEYSPACE}.order_line 
# (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, 
# ol_supply_w_id, ol_quantity, ol_dist_info, ol_i_name) 
# FROM '{DATA_DIR}/order-line-aug.csv' WITH DELIMITER=',' '''

# load_stock_q = \
# f'''COPY {KEYSPACE}.stock 
# (s_w_id, s_i_id, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, s_dist_01, s_dist_02, 
# s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, 
# s_data, s_i_name, s_i_price) 
# FROM '{DATA_DIR}/stock-aug.csv' WITH DELIMITER=',' '''

# load_qs = [load_warehouse_q, load_district_q, load_customer_q, load_orders_q, load_item_q, load_order_line_q, load_stock_q]

# cluster = Cluster(contact_points=[ip]*20, connect_timeout=100)
# sess = cluster.connect()
# sess.default_timeout = 300.0
# # print(sess.default_timeout)

# sess.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")
# time.sleep(60)
# sess.execute(f"CREATE KEYSPACE {KEYSPACE} WITH replication = {{'class': '{REP_STRATEGY}', 'replication_factor' : {REP_FACTOR}}}")
# sess.execute(f"USE {KEYSPACE}")
# print('Keyspace is reset')

# sess.execute(create_warehouse_q)
# sess.execute(create_district_q)
# sess.execute(create_customer_q)
# sess.execute(create_orders_q)
# sess.execute(create_item_q)
# sess.execute(create_order_line_q)
# sess.execute(create_stock_q)
# sess.execute(create_view_customer_top_balance_q)
# sess.execute(create_view_order_by_customer_q)
# sess.execute(create_order_line_by_item_q)
# print('Tables and views are created')

# # augment customer with w_name and d_name
# cols = 'c_w_id,c_d_id,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data'.split(',')
# df_customer = pd.read_csv(DATA_DIR + '/customer.csv', names=cols)
# print(df_customer.shape)

# cols = 'w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd'.split(',')
# df_warehouse = pd.read_csv(DATA_DIR + '/warehouse.csv', names=cols)

# cols = 'd_w_id,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id'.split(',')
# df_district = pd.read_csv(DATA_DIR + '/district.csv', names=cols)

# cols = 'c_w_id,c_d_id,c_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data,w_name,d_name'.split(',')
# df_wd = pd.merge(df_district, df_warehouse, how='left', left_on='d_w_id', right_on='w_id')
# df_wd = df_wd[['d_w_id', 'd_id', 'd_name', 'w_name']]
# df_customer_aug = pd.merge(df_customer, df_wd, how='left', left_on=['c_w_id', 'c_d_id'], right_on=['d_w_id', 'd_id'])
# df_customer_aug = df_customer_aug[cols]
# print(df_customer_aug.shape)

# df_customer_aug.to_csv(DATA_DIR + '/customer-aug.csv', index=False, header=False)
# print('customer aug is created')

# # augment order_line with i_name
# cols = 'ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info'.split(',')
# df_order_line = pd.read_csv(DATA_DIR + '/order-line.csv', names=cols)
# print(df_order_line.shape)

# cols = 'i_id,i_name,i_price,i_im_id,i_data'.split(',')
# df_item = pd.read_csv(DATA_DIR + '/item.csv', names=cols)

# cols = 'ol_w_id,ol_d_id,ol_o_id,ol_number,ol_i_id,ol_delivery_d,ol_amount,ol_supply_w_id,ol_quantity,ol_dist_info,i_name'.split(',')
# df_order_line_aug = pd.merge(df_order_line, df_item, how='left', left_on='ol_i_id', right_on='i_id')
# df_order_line_aug = df_order_line_aug[cols]
# print(df_order_line_aug.shape)

# df_order_line_aug.to_csv(DATA_DIR + '/order-line-aug.csv', index=False, header=False)
# print('order line aug is created')

# # augment stock with i_name and i_price
# cols = 's_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data'.split(',')
# df_stock = pd.read_csv(DATA_DIR + '/stock.csv', names=cols)
# print(df_stock.shape)

# cols = 'i_id,i_name,i_price,i_im_id,i_data'.split(',')
# df_item = pd.read_csv(DATA_DIR + '/item.csv', names=cols)

# cols = 's_w_id,s_i_id,s_quantity,s_ytd,s_order_cnt,s_remote_cnt,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_data,i_name,i_price'.split(',')
# df_stock_aug = pd.merge(df_stock, df_item, how='left', left_on='s_i_id', right_on='i_id')
# df_stock_aug = df_stock_aug[cols]
# print(df_stock_aug.shape)

# df_stock_aug.to_csv(DATA_DIR + '/stock-aug.csv', index=False, header=False)
# print('stock aug is created')

# # augment district with d_next_deliver_o_id
# map_dict = {}
# with open(DATA_DIR + '/order.csv') as f:
#     csv_reader = csv.reader(f)
#     for row in csv_reader:
#         key = (int(row[0]), int(row[1]))
#         if row[4] == "":
#             if key not in map_dict:
#                 map_dict[key] = int(row[2])

# deliver_ids = []
# for w_id in range(1, 11):
#     for d_id in range(1, 11):
#         deliver_ids.append([w_id, d_id, map_dict[(w_id, d_id)]])

# df_deliver = pd.DataFrame(deliver_ids, columns=['w_id', 'did', 'deliver_id'])
        
# cols = 'd_w_id,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id'.split(',')
# df_district = pd.read_csv(DATA_DIR + '/district.csv', names=cols)
# print(df_district.shape)

# cols = 'd_w_id,d_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id,deliver_id'.split(',')
# df_district_aug = pd.merge(df_district, df_deliver, how='left', left_on=['d_w_id', 'd_id'], right_on=['w_id', 'did'])
# df_district_aug = df_district_aug[cols]
# print(df_district_aug.shape)

# df_district_aug.to_csv(DATA_DIR + '/district-aug.csv', index=False, header=False)
# print('district aug is created')

# for q in load_qs:
#     comm = CQL_DIR + ' ' + ip +' -e "' + q + '"'
# #     print(comm)
#     res = os.popen(comm)
#     out = res.read()
# #     print(res.read())
# print('All data are loaded')
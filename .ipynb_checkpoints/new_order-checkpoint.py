# new order xact
from datetime import datetime

class NewOrder():
    def __init__(self, sess, level):
        self.sess = sess
        if level == 'ONE':
            self.pro = ['one', 'all']
        elif level == 'QUORUM':
            self.pro = ['quorum', 'quorum']
        self.pre_get_district = sess.prepare(
            "SELECT d_next_o_id, d_tax FROM district WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_update_next_id = sess.prepare(
            "UPDATE district SET d_next_o_id = ? WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_get_warehouse = sess.prepare(
            "SELECT w_tax FROM warehouse WHERE w_id = ?"
        )
        self.pre_get_customer = sess.prepare(
            "SELECT c_w_id, c_d_id, c_id, c_last, c_credit, c_discount FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        )
        self.pre_insert_order = sess.prepare(
            "INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        self.pre_get_stock = sess.prepare(
            "SELECT s_i_name, s_i_price, s_quantity, s_ytd, s_order_cnt, s_remote_cnt FROM stock WHERE s_w_id = ? AND s_i_id = ?"
        )
        self.pre_update_stock = sess.prepare(
            "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_w_id = ? AND s_i_id = ?"
        )
        self.pre_insert_order_line = sess.prepare(
            "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info, ol_i_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        
    def get_district(self, w_id, d_id):
        rows = self.sess.execute(self.pre_get_district.bind((w_id, d_id)), execution_profile=self.pro[0])
        return rows.one()
    
    def inc_next_id(self, w_id, d_id, new_id):
        self.sess.execute(self.pre_update_next_id.bind((new_id, w_id, d_id)), execution_profile=self.pro[1])
    
    def get_customer(self, w_id, d_id, c_id):
        rows = self.sess.execute(self.pre_get_customer.bind((w_id, d_id, c_id)), execution_profile=self.pro[0])
        return rows.one()
    
    def get_warehouse(self, w_id):
        rows = self.sess.execute(self.pre_get_warehouse.bind((w_id,)), execution_profile=self.pro[0])
        return rows.one()
    
    def insert_order(self, w_id, d_id, c_id, o_id, o_entry_d, num_items, supplier_warehouse_arr):
        local_arr = [1 if sup_w_id == w_id else 0 for sup_w_id in supplier_warehouse_arr]
        all_local = min(local_arr)
        self.sess.execute(self.pre_insert_order.bind((o_id, d_id, w_id, c_id, o_entry_d, num_items, all_local)), , execution_profile=self.pro[1])
        return o_entry_d
    
    def update_stock_and_order_line(self, w_id, d_id, o_id, num_items, item_number_arr, supplier_warehouse_arr, quantity_arr, w_tax, d_tax, c_discount):
        total_amount = 0
        items_info = []
        
        for i in range(num_items):
            i_id = item_number_arr[i]
            sup_w_id = supplier_warehouse_arr[i]
            qty = quantity_arr[i]
            
            # update stock
            rows = self.sess.execute(self.pre_get_stock.bind((sup_w_id, i_id)), execution_profile=self.pro[0])
            stock = rows.one()
            
            adjusted_qty = stock.s_quantity - qty
            if adjusted_qty < 10:
                adjusted_qty += 100
            new_s_ytd = stock.s_ytd + qty
            new_s_order_cnt = stock.s_order_cnt + 1
            new_s_remote_cnt = stock.s_remote_cnt + 1 if sup_w_id != w_id else 0
            
            self.sess.execute(self.pre_update_stock.bind((adjusted_qty, new_s_ytd, new_s_order_cnt, new_s_remote_cnt, sup_w_id, i_id)), execution_profile=self.pro[1])
            
            item_amount = qty * stock.s_i_price
            total_amount += item_amount
            
            # update order line
            ol_num = i + 1
            dist = 'S_DIST_{:0d}'.format(d_id)
            self.sess.execute(self.pre_insert_order_line.bind((o_id, d_id, w_id, ol_num, i_id, sup_w_id, qty, item_amount, dist, stock.s_i_name)), execution_profile=self.pro[1])
            
            info = {
                'item_number': i_id,
                'i_name': stock.s_i_name,
                'supplier_warehouse': sup_w_id,
                'quantity': qty,
                'ol_amount': float(item_amount),
                's_quantity': int(stock.s_quantity)
            }
            items_info.append(info)
        
        total_amount = total_amount * (1 + d_tax + w_tax) * (1 - c_discount)
        return total_amount, items_info
        
    def exec_xact(self, c_id, w_id, d_id, num_items, item_number_arr, supplier_warehouse_arr, quantity_arr):
        o_entry_d = datetime.now()
    
        district = self.get_district(w_id, d_id)
        o_id, d_tax = district.d_next_o_id, district.d_tax

        self.inc_next_id(w_id, d_id, o_id+1)
        
        warehouse = self.get_warehouse(w_id)
        w_tax = warehouse.w_tax
        
        customer = self.get_customer(w_id, d_id, c_id)
        c_discount = customer.c_discount
        
        o_entry_d = self.insert_order(w_id, d_id, c_id, o_id, o_entry_d, num_items, supplier_warehouse_arr)
        total_amount, items_info = self.update_stock_and_order_line(w_id, d_id, o_id, num_items, item_number_arr, supplier_warehouse_arr, quantity_arr, w_tax, d_tax, c_discount)
        
        res = {
            'customer_identifier': (w_id, d_id, c_id),
            'c_last': customer.c_last,
            'c_credit': customer.c_credit,
            'c_discount': float(customer.c_discount),
            'w_tax': float(w_tax),
            'd_tax': float(d_tax),
            'o_id': o_id,
            'o_entry_d': str(o_entry_d),
            'num_items': num_items,
            'total_amount': float(total_amount),
            'items_info': items_info
        }
        return res
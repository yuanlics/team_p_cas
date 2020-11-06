# popular item xact
class PopularItem():
    def __init__(self, sess):
        self.sess = sess
        self.pre_get_orders = sess.prepare(
            "SELECT o_id, o_c_id, o_entry_d FROM orders WHERE o_w_id = ? AND o_d_id = ? LIMIT ?"
        )
        self.pre_get_customer = sess.prepare(
            '''SELECT c_first, c_middle, c_last FROM customer
            WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'''
        )
        self.pre_get_top_quantity = sess.prepare(
            '''SELECT ol_quantity FROM order_line
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? LIMIT 1'''
        )
        self.pre_get_items = sess.prepare(
            '''SELECT ol_i_id, ol_i_name, ol_quantity FROM order_line
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ?'''
        )
        
    def get_orders(self, w_id, d_id, order_num):
        rows = self.sess.execute(self.pre_get_orders.bind((w_id, d_id, order_num)))
        return list(rows)

    def get_customer(self, w_id, d_id, c_id):
        rows = self.sess.execute(self.pre_get_customer.bind((w_id, d_id, c_id)))
        return rows.one()
    
    def get_popular_items(self, w_id, d_id, o_id):
        rows = self.sess.execute(self.pre_get_top_quantity.bind((w_id, d_id, o_id))).one()
        if rows is None:
            raise Exception('can not find top quantity')
        rows = self.sess.execute(self.pre_get_items.bind((w_id, d_id, o_id, rows.ol_quantity)))
        return list(rows)

    def exec_xact(self, w_id, d_id, order_num):
        orders = self.get_orders(w_id, d_id, order_num)
        order_cnt = len(orders)

        orders_list = []
        items_dict = {}
        for order in orders:
            custom = self.get_customer(w_id, d_id, order.o_c_id)
            items = self.get_popular_items(w_id, d_id, order.o_id)
            if items is None:
                raise Exception('can not find popular items')
            items_list = []
            for item in items:
                i_id = item.ol_i_id
                if i_id not in items_dict:
                    items_dict[i_id] = [item.ol_i_name, 1]
                else:
                    items_dict[i_id][1] += 1
                
                if item.ol_quantity is None:
                    q = 0
                else:
                    q = item.ol_quantity
                items_list.append({
                    'i_name': item.ol_i_name,
                    'i_quantity': float(q),
                })

            orders_list.append({
                'order': {'o_id': order.o_id, 
                          'o_entry_d': str(order.o_entry_d), 
                          'o_c_id': order.o_c_id},
                'customer': {'c_first': custom.c_first,
                             'c_middle': custom.c_middle,
                             'c_last': custom.c_last},
                'items': items_list
            })
        
        percent = []
        for i_id in items_dict:
            percent.append({
                'i_name': items_dict[i_id][0],
                'i_percentage': items_dict[i_id][1] / order_cnt
            })

        res = {
            'district_identifier': {'w_id': w_id, 'd_id': d_id},
            'order_num': order_num,
            'orders_detail': orders_list,
            'item_percentage': percent
        }
        return res
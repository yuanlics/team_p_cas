# related customer xact
from collections import defaultdict

class RelatedCustomer():
    def __init__(self, sess, level):
        self.sess = sess
        if level == 'ONE':
            self.pro = ['one', 'all']
        elif level == 'QUORUM':
            self.pro = ['quorum', 'quorum']
        self.pre_get_orders_by_customer = sess.prepare(
            "SELECT o_id FROM order_sort_by_customer WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?;"
        )
        self.pre_get_order_lines = sess.prepare(
            "SELECT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?"
        )
        self.pre_get_remote_order_lines_by_selected_items = sess.prepare(
            "SELECT ol_w_id, ol_d_id, ol_o_id, ol_i_id FROM order_line_sort_by_item WHERE ol_w_id IN ? AND ol_d_id IN (1,2,3,4,5,6,7,8,9,10) AND ol_i_id IN ?"
        )
        self.pre_get_customer_by_order = sess.prepare(
            "SELECT o_c_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
        )
        
    def get_related_customer(self, other_ws, i_ids):
        rows = self.sess.execute(self.pre_get_remote_order_lines_by_selected_items.bind((other_ws, i_ids)), execution_profile=self.pro[0])
        
        common_item_count = defaultdict(set)  # {(w_id, d_id, o_id): {common_i_ids}}
        for row in rows:
            common_item_count[(row.ol_w_id, row.ol_d_id, row.ol_o_id)].add(row.ol_i_id)

        at_least_two_common = list(filter(lambda p: len(p[1]) >= 2, common_item_count.items()))
        for (ol_w_id, ol_d_id, ol_o_id), _ in at_least_two_common:
            related_customer = self.sess.execute(self.pre_get_customer_by_order.bind((ol_w_id, ol_d_id, ol_o_id)), execution_profile=self.pro[0]).one()
            if related_customer is None:
                continue
            rel_c_id = related_customer.o_c_id
            self.related_customers.add((ol_w_id, ol_d_id, rel_c_id))
        
    def exec_xact(self, w_id, d_id, c_id):
        orders = self.sess.execute(self.pre_get_orders_by_customer.bind((w_id, d_id, c_id)), execution_profile=self.pro[0])
        self.related_customers = set()
        for order in orders:
            order_lines = self.sess.execute(self.pre_get_order_lines.bind((w_id, d_id, order.o_id)), execution_profile=self.pro[0])
            other_ws = {i for i in range(1, 11)} - {w_id}
            i_ids = {ol.ol_i_id for ol in order_lines}
            self.get_related_customer(other_ws, i_ids)
            
        res = {
            'customer_identifier': (w_id, d_id, c_id),
            'related_customers_identifier': list(self.related_customers)
        }
        return res
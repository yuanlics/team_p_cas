# order status xact
class OrderStatus():
    def __init__(self, sess):
        self.sess = sess
        self.pre_get_customer = sess.prepare(
            '''SELECT c_first, c_middle, c_last, c_balance FROM customer
            WHERE c_w_id = ? AND c_d_id = ? and c_id = ?'''
        )
        self.pre_get_order = sess.prepare(
            '''SELECT o_id, o_carrier_id, o_entry_d FROM order_sort_by_customer
            WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? LIMIT 1'''
        )
        self.pre_get_items = sess.prepare(
            '''SELECT ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity FROM order_line
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?'''
        )

    def get_customer(self, w_id, d_id, c_id):
        rows = self.sess.execute(self.pre_get_customer.bind((w_id, d_id, c_id)))
        return rows.one()

    def get_order(self, w_id, d_id, c_id):
        rows = self.sess.execute(self.pre_get_order.bind((w_id, d_id, c_id)))
        return rows.one()

    def get_items(self, w_id, d_id, o_id):
        rows = self.sess.execute(self.pre_get_items.bind((w_id, d_id, o_id)))
        return rows

    def exec_xact(self, w_id, d_id, c_id):
        customer = self.get_customer(w_id, d_id, c_id)
        order = self.get_order(w_id, d_id, c_id)
        items = self.get_items(w_id, d_id, order.o_id)
        item_list = []
        for item in items:
            item_list.append({
                'ol_i_id': item.ol_i_id,
                'ol_supply_w_id': item.ol_supply_w_id,
                'ol_quantity': float(item.ol_quantity),
                'ol_amount': float(item.ol_amount),
                'ol_delivery_d': str(item.ol_delivery_d)
            })
        res = {
            'customer_name': {'c_first': customer.c_first,
                              'c_middle': customer.c_middle,
                              'c_last': customer.c_last,
                              'c_balance': float(customer.c_balance)},
            'order_detail': {'o_id': order.o_id,
                             'o_entry_d': str(order.o_entry_d),
                             'o_carrier_id': order.o_carrier_id},
            'items': item_list
        }
        return res
# delivery xact
from datetime import datetime

class Delivery():
    def __init__(self, sess):
        self.sess = sess
        self.pre_get_deliver_id = sess.prepare(
            "SELECT d_next_o_id, d_next_deliver_o_id FROM district WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_update_deliver_id = sess.prepare(
            "UPDATE district SET d_next_deliver_o_id = ? WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_update_carrier_id = sess.prepare(
            "UPDATE orders SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
        )
        self.pre_get_items_info = sess.prepare(
            '''SELECT ol_number, ol_amount, ol_quantity, ol_i_id FROM order_line
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?'''
        )
        self.pre_update_ol_delivery_d = sess.prepare(
            '''UPDATE order_line SET ol_delivery_d = ? 
            WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ? AND ol_quantity = ? AND ol_number = ? AND ol_i_id = ?'''
        )
        self.pre_get_customer_id = sess.prepare(
            "SELECT o_c_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?"
        )
        self.pre_get_customer_info = sess.prepare(
            "SELECT c_balance, c_delivery_cnt FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        )
        self.pre_update_customer = sess.prepare(
            "UPDATE customer SET c_balance = ?, c_delivery_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        )

    def process_next_deliver_id(self, w_id, d_id):
        rows = self.sess.execute(self.pre_get_deliver_id.bind((w_id, d_id)))
        next_order_id = rows.one().d_next_o_id
        next_deliver_id = rows.one().d_next_deliver_o_id
        if next_order_id <= next_deliver_id:
            return None
        self.sess.execute(self.pre_update_deliver_id.bind((next_deliver_id+1, w_id, d_id)))
        return next_deliver_id

    def update_carrier_id(self, w_id, d_id, o_id, carrier_id):
        self.sess.execute(self.pre_update_carrier_id.bind((carrier_id, w_id, d_id, o_id)))

    def update_datetime(self, w_id, d_id, o_id):
        rows = self.sess.execute(self.pre_get_items_info.bind((w_id, d_id, o_id)))
        total_amount = 0
        for item in rows:
            total_amount += float(item.ol_amount)
            self.sess.execute(self.pre_update_ol_delivery_d.bind(
            (datetime.now(), w_id, d_id, o_id, float(item.ol_quantity), item.ol_number, item.ol_i_id)))
        return total_amount

    def update_customer(self, w_id, d_id, o_id, total_amount):
        rows = self.sess.execute(self.pre_get_customer_id.bind((w_id, d_id, o_id)))
        c_id = rows.one().o_c_id
        rows = self.sess.execute(self.pre_get_customer_info.bind((w_id, d_id, c_id)))
        c_balance = rows.one().c_balance
        c_delivery_cnt = rows.one().c_delivery_cnt
        self.sess.execute(self.pre_update_customer.bind((float(c_balance)+total_amount,c_delivery_cnt+1, w_id, d_id, c_id)))

    def exec_xact(self, w_id, carrier_id):
        for d_id in range(1, 11):
            next_id = self.process_next_deliver_id(w_id, d_id)
            if next_id is not None:
                self.update_carrier_id(w_id, d_id, next_id, carrier_id)
                total_amount = self.update_datetime(w_id, d_id, next_id)
                self.update_customer(w_id, d_id, next_id, total_amount)
            else:
                print('delivery fail')
        return 'delivery done'
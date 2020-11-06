# stock level xact
class StockLevel():
    def __init__(self, sess):
        self.sess = sess
        self.pre_get_next_id = sess.prepare(
            "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_get_item_ids = sess.prepare(
            "SELECT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id >= ? AND ol_o_id < ?"
        )
        self.pre_get_quantity = sess.prepare(
            "SELECT s_quantity FROM stock WHERE s_w_id = ? AND s_i_id = ?"
        )

    def get_next_id(self, w_id, d_id):
        rows = self.sess.execute(self.pre_get_next_id.bind((w_id, d_id)))
        return rows.one().d_next_o_id

    def get_item_ids(self, w_id, d_id, min_id, max_id):
        rows = self.sess.execute(self.pre_get_item_ids.bind((w_id, d_id, min_id, max_id)))
        return list(set([row.ol_i_id for row in rows]))

    def get_below_item_num(self, w_id, i_ids, thres):
        cnt = 0
        for i_id in i_ids:
            rows = self.sess.execute(self.pre_get_quantity.bind((w_id, i_id)))
            if rows.one().s_quantity < thres:
                cnt += 1
        return cnt

    def exec_xact(self, w_id, d_id, thres, order_num):
        next_id = self.get_next_id(w_id, d_id)
        i_ids = self.get_item_ids(w_id, d_id, next_id-order_num, next_id)
        res = self.get_below_item_num(w_id, i_ids, thres)
        return res
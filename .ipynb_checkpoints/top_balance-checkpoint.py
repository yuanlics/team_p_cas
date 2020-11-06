# top balance xact
class TopBalance():
    def __init__(self, sess, level):
        self.sess = sess
        if level == 'ONE':
            self.pro = ['one', 'all']
        elif level == 'QUORUM':
            self.pro = ['quorum', 'quorum']
        self.pre_get_top_customers = sess.prepare(
            '''SELECT c_w_id, c_balance, c_d_id, c_first, c_middle, c_last, c_w_name, c_d_name
            FROM customer_sort_by_balance
            WHERE c_w_id = ? LIMIT ?'''
        )
        
    def get_top_customers(self, w_id, num):
        rows = self.sess.execute(self.pre_get_top_customers.bind((w_id, num)), execution_profile=self.pro[0])
        return list(rows)

    def exec_xact(self):
        num = 10
        customers = []
        for w_id in range(1, 11):
            customers += self.get_top_customers(w_id, num)

        customers.sort(key=lambda c: c.c_balance)
        res = []
        for c in customers[:num]:
            w_name = c.c_w_name
            d_name = c.c_d_name
            c_dict = {
                'customer_name': {'c_first': c.c_first,
                                  'c_middle': c.c_middle,
                                  'c_last': c.c_last},
                'balance': float(c.c_balance),
                'warehouse_name': w_name,
                'district_name': d_name
            }
            res.append(c_dict)
        return res
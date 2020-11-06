# payment xact
class Payment():
    def __init__(self, sess, level):
        self.sess = sess
        if level == 'ONE':
            self.pro = ['one', 'all']
        elif level == 'QUORUM':
            self.pro = ['quorum', 'quorum']
        self.pre_get_warehouse = sess.prepare(
            "SELECT w_ytd, w_street_1, w_street_2, w_city, w_state, w_zip FROM warehouse WHERE w_id = ?"
        )
        self.pre_update_warehouse = sess.prepare(
            "UPDATE warehouse SET w_ytd = ? WHERE w_id = ?"
        )
        self.pre_get_district = sess.prepare(
            "SELECT d_ytd, d_street_1, d_street_2, d_city, d_state, d_zip FROM district WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_update_district = sess.prepare(
            "UPDATE district SET d_ytd = ? WHERE d_w_id = ? AND d_id = ?"
        )
        self.pre_get_customer = sess.prepare(
            '''SELECT c_balance, c_ytd_payment, c_payment_cnt, c_first, c_middle, c_last, c_street_1, c_street_2, 
            c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance
            FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'''
        )
        self.pre_update_customer = sess.prepare(
            "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?"
        )
        
    def get_warehouse(self, w_id):
        rows = self.sess.execute(self.pre_get_warehouse.bind((w_id,)), execution_profile=self.pro[0])
        return rows.one()
    
    def update_warehouse(self, w_id, new_w_ytd):
        self.sess.execute(self.pre_update_warehouse.bind((new_w_ytd, w_id)), execution_profile=self.pro[1])
        
    def get_district(self, w_id, d_id):
        rows = self.sess.execute(self.pre_get_district.bind((w_id, d_id)), execution_profile=self.pro[0])
        return rows.one()
    
    def update_district(self, w_id, d_id, new_d_ytd):
        self.sess.execute(self.pre_update_district.bind((new_d_ytd, w_id, d_id)), execution_profile=self.pro[1])
         
    def get_customer(self, w_id, d_id, c_id):
        rows = self.sess.execute(self.pre_get_customer.bind((w_id, d_id, c_id)), execution_profile=self.pro[0])
        return rows.one()
    
    def update_customer(self, w_id, d_id, c_id, new_c_balance, new_c_ytd_payment, new_c_payment_cnt):
        self.sess.execute(self.pre_update_customer.bind((new_c_balance, new_c_ytd_payment, new_c_payment_cnt, w_id, d_id, c_id)), execution_profile=self.pro[1])    
        
    def exec_xact(self, w_id, d_id, c_id, payment):
        warehouse = self.get_warehouse(w_id)
        new_w_ytd = float(warehouse.w_ytd) + payment
        self.update_warehouse(w_id, new_w_ytd)
        
        district = self.get_district(w_id, d_id)
        new_d_ytd = float(district.d_ytd) + payment
        self.update_district(w_id, d_id, new_d_ytd)
        
        customer = self.get_customer(w_id, d_id, c_id)
        new_c_balance = float(customer.c_balance) - payment
        new_c_ytd_payment = float(customer.c_ytd_payment) + payment
        new_c_payment_cnt = customer.c_payment_cnt + 1
        self.update_customer(w_id, d_id, c_id, new_c_balance, new_c_ytd_payment, new_c_payment_cnt)
        
        w, d, c = warehouse, district, customer
        res = {
            'customer_identifier': (w_id, d_id, c_id),
            'customer_name': (c.c_first, c.c_middle, c.c_last),
            'customer_address': (c.c_street_1, c.c_street_2, c.c_city, c.c_state, c.c_zip),
            'c_phone': c.c_phone,
            'c_credit': c.c_credit,
            'c_credit_lim': c.c_credit_lim,
            'c_discount': float(c.c_discount),
            'c_balance': float(c.c_balance),
            'warehouse_address': (w.w_street_1, w.w_street_2, w.w_city, w.w_state, w.w_zip),
            'district_address': (d.d_street_1, d.d_street_2, d.d_city, d.d_state, d.d_zip),
            'payment': payment
        }
        return res
from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Image(MySQLBase):
    def add_photo(self, title, category_id, file_extension, user_id, date):
        sql = "insert into photo(title, category_id, file_extension, date, user_id) value(%s, %s, %s, %s, %s)"
        args = (title, category_id, file_extension, user_id, date, ROBOT)
        self.rdbms_pool.edit(sql, args)
from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Video(MySQLBase):
    def add_video(self, title, category_id, file_extension, date, description):
        sql = "insert into video(title, category_id, file_extension, date, description, user_id) value(%s, %s, %s, %s, %s, %s)"
        self.rdbms_pool.edit(sql, args=(title, category_id, file_extension, date, description, ROBOT))
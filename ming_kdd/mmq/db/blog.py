from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Article(MySQLBase):
    def add_article(self, title, is_public, content, date, category_id, url, downloaded):
        sql = ('insert ignore into article_s(title, category_id, is_public, content, date, url, user_id)'
               ' value(%s, %s, %s, %s, %s, %s, %s)')
        self.rdbms_pool.edit(sql, args=(title, is_public, content, date, category_id, url, downloaded, ROBOT))


class Image(MySQLBase):
    def add_photo(self, title, category_id, file_extension, user_id, date):
        sql = "insert into photo(title, category_id, file_extension, date, user_id) value(%s, %s, %s, %s, %s)"
        args = (title, category_id, file_extension, user_id, date, ROBOT)
        self.rdbms_pool.edit(sql, args)


class Video(MySQLBase):
    def add_video(self, title, category_id, file_extension, date, description):
        sql = "insert into video(title, category_id, file_extension, date, description, user_id) value(%s, %s, %s, %s, %s, %s)"
        self.rdbms_pool.edit(sql, args=(title, category_id, file_extension, date, description, ROBOT))
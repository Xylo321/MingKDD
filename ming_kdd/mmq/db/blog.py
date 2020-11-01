from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Article(MySQLBase):
    def add_article(self, title, is_public, content, date, category_id, url, downloaded):
        sql = ('insert ignore into article_s(title, category_id, is_public, content, date, url, user_id)'
               ' value(%s, %s, %s, %s, %s, %s, %s)')
        self.rdbms_pool.edit(sql, args=(title, is_public, content, date, category_id, url, downloaded, ROBOT))
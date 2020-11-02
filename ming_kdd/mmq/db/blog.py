from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Article(MySQLBase):
    def add_articles(self, args):
        sql = ('insert ignore into article_s(title, category_id, is_public, content, date, url, user_id)'
               ' value(%s, %s, %s, %s, %s, %s, %s)')
        self.rdbms_pool.edit_many(sql, args=args)


class Category(MySQLBase):
    def exist(self, category_id):
        sql = 'select id from category_s where id = %s'
        result =  self.rdbms_pool.query(sql, args=(category_id,))
        if result != None and len(result) != 0:
            return result[0]['id']
        else:
            return None
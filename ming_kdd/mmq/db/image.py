from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Image(MySQLBase):
    def add_photo(self, title, category_id, file_extension, date, data_src_id):
        sql = "insert into photo_s(title, category_id, file_extension, date, user_id, data_src_id) value(%s, %s, %s, %s, %s, %s)"
        self.rdbms_pool.edit(sql, args=(title, category_id, file_extension, date, ROBOT, data_src_id))

    def get_photo_id(self, title):
        sql = 'select id from photo_s where title = %s and user_id = %s'
        result = self.rdbms_pool.query(sql, args=(title, ROBOT))
        if result != None and len(result) != 0:
            return result[0]['id']
        else:
            return None

    def get_photo_download_status(self, photo_id):
        sql = 'select downloaded from photo_s where id = %s'
        result = self.rdbms_pool.query(sql, args=(photo_id, ))
        if result != None and len(result) != 0:
            return result[0]['downloaded']
        else:
            return None

    def change_photo_download_status(self, photo_id):
        sql = 'update photo_s set downloaded = 1 where id = %s'
        return self.rdbms_pool.edit(sql, args=(photo_id, ))


class Category(MySQLBase):
    def add_category(self, name):
        sql = 'insert into category_s(name, user_id) value(%s, %s)'
        affect_rows = self.rdbms_pool.edit(sql, args=(name, ROBOT))
        return affect_rows

    def get_category_id_by_name(self, name):
        sql = 'select id from category_s where name = %s'
        result = self.rdbms_pool.query(sql, args=(name,))
        if result != None and len(result) != 0:
            return result[0]['id']
        else:
            return None

    def del_category_by_category_id(self, category_id):
        sql = 'delete from category_s where id = %s'
        affect_rows = self.rdbms_pool.edit(sql, args=(category_id, ))
        return affect_rows


class DataSrc(MySQLBase):
    def get_id_by_web_site(self, web_site):
        sql = 'select id from data_src_s where web_site = %s'
        result = self.rdbms_pool.query(sql, args=(web_site, ))
        if result != None and len(result) != 0:
            return result[0]['id']
        else:
            return None
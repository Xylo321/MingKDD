from ming_kdd.mmq.db.rdbms import MySQLBase
from ming_kdd.mmq.settings.db import ROBOT


class Video(MySQLBase):
    def add_video(self, title, category_id, file_extension, date, description, data_src_id):
        sql = "insert into video_s(title, category_id, file_extension, date, description, user_id, data_src_id) value(%s, %s, %s, %s, %s, %s, %s)"
        self.rdbms_pool.edit(sql, args=(title, category_id, file_extension, date, description, ROBOT, data_src_id))

    def get_video_id(self, title):
        sql = 'select id from video_s where title = %s and user_id = %s'
        result = self.rdbms_pool.query(sql, args=(title, ROBOT))
        if result != None and len(result) != 0:
            return result[0]['id']
        else:
            return None

    def get_video_download_status(self, video_id):
        sql = 'select downloaded from video_s where id = %s'
        result = self.rdbms_pool.query(sql, args=(video_id, ))
        if result != None and len(result) != 0:
            return result[0]['downloaded']
        else:
            return None

    def change_video_download_status(self, video_id):
        sql = 'update video_s set downloaded = 1 where id = %s'
        return self.rdbms_pool.edit(sql, args=(video_id, ))


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
import logging
import traceback
from collections import deque
from threading import Lock

import pymysql


class MySQLPool:
    _LOCK = Lock()
    _LOGGER = logging.getLogger('MySQLPool')

    def __init__(self, host, user, passwd, db, size):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.size = size
        self.pool = deque()

        self._init_pool()

    def _init_pool(self):
        for i in range(self.size):
            conn = self._get_conn()
            if conn is not None:
                self.pool.append(conn)

    def _get_conn(self):
        try:
            connection = pymysql.connect(host=self.host,
                                         user=self.user,
                                         password=self.passwd,
                                         db=self.db,
                                         charset='utf8mb4',
                                         write_timeout=60,
                                         read_timeout=60,
                                         connect_timeout=60,
                                         # autocommit=True,
                                         cursorclass=pymysql.cursors.DictCursor)
            return connection
        except:
            MySQLPool._LOGGER.error(traceback.format_exc())
            return None

    def get_conn(self):
        with MySQLPool._LOCK:
            try:
                pool_size = len(self.pool)
                i = 0
                while i < pool_size:
                    try:
                        conn = self.pool.popleft()
                        conn.ping()
                        return conn
                    except:
                        MySQLPool._LOGGER.error(traceback.format_exc())
                    finally:
                        i += 1

                raise Exception('连接池已为空')
            except:
                self.release()
                try:
                    self._init_pool()
                except:
                    MySQLPool._LOGGER.error(traceback.format_exc())

                pool_size = len(self.pool)
                i = 0
                while i < pool_size:
                    conn = self.pool.popleft()
                    try:
                        conn.ping()
                        return conn
                    except:
                        MySQLPool._LOGGER.error(traceback.format_exc())
                    finally:
                        i += 1

                raise Exception('我已经尽力了。')

    def back_conn(self, conn):
        with MySQLPool._LOCK:
            self.pool.append(conn)

    def release(self):
        for conn in self.pool:
            try:
                conn.close()
            except:
                MySQLPool._LOGGER.error(traceback.format_exc())

        self.pool.clear()

    def query(self, sql, args):
        conn = None
        try:
            conn = self.get_conn()

            with conn.cursor() as cursor:
                cursor.execute(sql, args)

                return cursor.fetchall()
        except:
            MySQLPool._LOGGER.error(traceback.format_exc())
            MySQLPool._LOGGER.error(sql)
            MySQLPool._LOGGER.error(args)
            conn = None
        finally:
            if conn: self.back_conn(conn)

    def edit(self, sql, args):
        affect_rows = 0
        conn = None
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor.execute(sql, args)
            conn.commit()
            affect_rows = conn.affected_rows()
        except:
            MySQLPool._LOGGER.error(traceback.format_exc())
            MySQLPool._LOGGER.error(sql)
            MySQLPool._LOGGER.error(args)
            try:
                conn.rollback()
            except Exception as rollback_err:
                MySQLPool._LOGGER.error('回滚时出现错误: %s', str(rollback_err))

            conn = None
        finally:
            if conn: self.back_conn(conn)
            return affect_rows

    def edit_many(self, sql, many_args):
        affect_rows = 0
        conn = None
        try:
            conn = self.get_conn()
            with conn.cursor() as cursor:
                cursor._do_execute_many(sql, many_args)
            conn.commit()
            affect_rows = conn.affected_rows()
        except:
            MySQLPool._LOGGER.error(sql)
            MySQLPool._LOGGER.error(many_args)
            try:
                conn.rollback()
            except Exception as rollback_err:
                MySQLPool._LOGGER.error('回滚时出现错误: %s', str(rollback_err))

            conn = None
        finally:
            if conn: self.back_conn(conn)
            return affect_rows


class MySQLBase(object):
    def __init__(self, rdbms_pool):
        self.rdbms_pool: MySQLPool = rdbms_pool
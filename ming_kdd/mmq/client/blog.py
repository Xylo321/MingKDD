from ming_kdd.mmq.settings.db import BLOG_MYSQL_CONFIG
from ming_kdd.mmq.db.rdbms import MySQLPool
from ming_kdd.mmq.db.blog import Article
from ming_kdd.mmq.settings.mq_cli import MINGMQ_CONFIG
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
import json
import logging
import traceback
import time


_BLOG_MYSQL_POOL: MySQLPool = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('blog_get_article_category_consumer')


def _init_mysql_pool() -> None:
    global _IMAGE_MYSQL_POOL, _VIDEO_MYSQL_POOL
    _BLOG_MYSQL_POOL = MySQLPool(host=BLOG_MYSQL_CONFIG['host'],
                                  user=BLOG_MYSQL_CONFIG['user'],
                                  passwd=BLOG_MYSQL_CONFIG['passwd'],
                                  db=BLOG_MYSQL_CONFIG['db'],
                                  size=BLOG_MYSQL_CONFIG['size'])

def _release_mysql_pool() -> None:
    global _BLOG_MYSQL_POOL
    _BLOG_MYSQL_POOL.release()


def _init_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['get_article_category']['host'],
                              MINGMQ_CONFIG['get_article_category']['port'],
                              MINGMQ_CONFIG['get_article_category']['user_name'],
                              MINGMQ_CONFIG['get_article_category']['passwd'],
                              MINGMQ_CONFIG['get_article_category']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER
    _MINGMQ_POOL.opera('declare_queue', *(queue_name,))
    while True:
        mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
        _LOGGER.debug('从消息队列中获取的消息为: %s', mq_res)

        if mq_res and mq_res['status'] != FAIL:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            user_id: int = message_data['user_id']
            category_id: int = message_data['category_id']
            db_name: str = message_data['db_name']
            b: bool = _delete_file_by_category_id_db_name(category_id, user_id, db_name)
            if b == True:
                message_id = mq_res['json_obj'][0]['message_id']
                mq_res = _MINGMQ_POOL.opera('ack_message', *(queue_name, message_id))
                if mq_res and mq_res['status'] != FAIL:
                    _LOGGER.debug('消息确认成功')
                else:
                    _LOGGER.error('消息确认失败: queue_name=%s, message_id=%s', queue_name, message_id)
        else:
            time.sleep(10)


def main(debug=logging.DEBUG) -> None:
    logging.basicConfig(level=debug)
    global _LOGGER
    try:
        _init_mysql_pool()
        _init_mingmq_pool()

        _get_data_from_queue(MINGMQ_CONFIG['get_article_category']['queue_name'])
    except:
        _LOGGER.error(traceback.format_exc())
        try:
            _release_mingmq_pool()
            _release_mysql_pool()
        except:
            _LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    main()
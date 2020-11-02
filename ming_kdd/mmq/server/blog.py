import logging
import traceback
from threading import Thread, Lock
import time

from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.mmq.settings.db import BLOG_MYSQL_CONFIG
from ming_kdd.mmq.db.blog import Article, Category
from ming_kdd.mmq.settings.db import ROBOT
from ming_kdd.mmq.db.rdbms import MySQLPool


_BLOG_MYSQL_POOL = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('blog_get_article_category_consumer')


def _init_mysql_pool() -> None:
    global _BLOG_MYSQL_POOL
    _BLOG_MYSQL_POOL = MySQLPool(host=BLOG_MYSQL_CONFIG['host'],
                                  user=BLOG_MYSQL_CONFIG['user'],
                                  passwd=BLOG_MYSQL_CONFIG['passwd'],
                                  db=BLOG_MYSQL_CONFIG['db'],
                                  size=BLOG_MYSQL_CONFIG['size'])

def _release_mysql_pool():
    global _BLOG_MYSQL_POOL
    _BLOG_MYSQL_POOL.release()

def _init_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['add_article']['host'],
                              MINGMQ_CONFIG['add_article']['port'],
                              MINGMQ_CONFIG['add_article']['user_name'],
                              MINGMQ_CONFIG['add_article']['passwd'],
                              MINGMQ_CONFIG['add_article']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()


def _task(mq_res, queue_name, lock, sig):
    global _BLOG_MYSQL_POOL

    with lock:
        sig -= 1

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = mq_res['json_obj'][0]['message_data']
            category_id: int = message_data['category_id']
            message: list = message_data['message']
            category = Category(_BLOG_MYSQL_POOL)
            article = Article(_BLOG_MYSQL_POOL)
            args = []
            if category.exist(category_id) is not None:
                for mes in message:
                    title = mes['title']
                    is_public = 1
                    url = mes['url']
                    content = '# %s\n <iframe src="%s"></iframe>' % (title, url)
                    date = int(time.time())
                    args.append((title, category_id, is_public, content, date, url, ROBOT))

                article.add_articles(args)
                _LOGGER.debug('增加到mysql中到数据为: %s', str(args))

            b = True
        except Exception as e:
            _LOGGER.debug('XX: 失败，数据存储到mysql: %s，错误信息: %s', str(mq_res), str(e))
        finally:
            if b == True:
                message_id = mq_res['json_obj'][0]['message_id']
                try:
                    mq_res = _MINGMQ_POOL.opera('ack_message', *(queue_name, message_id))
                    if mq_res and mq_res['status'] != FAIL:
                        _LOGGER.debug('消息确认成功')
                    else:
                        _LOGGER.error('消息确认失败: queue_name=%s, message_id=%s', queue_name, message_id)
                except Exception as e:
                    _LOGGER.debug('XX: 失败，消息确认失败: %s，错误信息: %s', str(message), str(e))
            with lock:
                sig += 1


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER
    _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['add_article']['queue_name'],))

    sig = MINGMQ_CONFIG['add_article']['pool_size']
    lock = Lock()

    while True:
        if sig != 0:
            try:
                mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
                _LOGGER.debug('从消息队列中获取的消息为: %s', mq_res)
            except Exception as e:
                _LOGGER.debug('XX: 从消息队列中获取任务失败，错误信息: %s', str(e))
            try:
                Thread(target=_task, args=(mq_res, queue_name, lock, sig)).start()
            except Exception as e:
                _LOGGER.debug("XX: 线程在执行过程中出现异常，错误信息为: %s", str(e))
        time.sleep(10)


def main(debug=logging.DEBUG) -> None:
    logging.basicConfig(level=debug)
    global _LOGGER
    try:
        _init_mysql_pool()
        _init_mingmq_pool()
        _get_data_from_queue(MINGMQ_CONFIG['add_article']['queue_name'])
    except:
        _LOGGER.error(traceback.format_exc())
    finally:
        try:
            _release_mingmq_pool()
        except:
            _LOGGER.error(traceback.format_exc())

        try:
            _release_mysql_pool()
        except:
            _LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
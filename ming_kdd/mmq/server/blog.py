import json
import logging
import traceback
from threading import Thread, Lock
import time
import sys

from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.mmq.settings.db import BLOG_MYSQL_CONFIG
from ming_kdd.mmq.db.blog import Article, Category
from ming_kdd.mmq.settings.db import ROBOT
from ming_kdd.mmq.db.rdbms import MySQLPool


_VIDEO_MYSQL_POOL = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('blog_add_article_consumer')


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


def _task(mq_res, queue_name):
    global _BLOG_MYSQL_POOL, SIG, LOCK

    with LOCK:
        SIG -= 1

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            category_id: int = message_data['category_id']
            message: list = message_data['message']
            category = Category(_BLOG_MYSQL_POOL)
            article = Article(_BLOG_MYSQL_POOL)
            args = []
            if category.exist(category_id) is not None:
                for mes in message:
                    title = mes['title']
                    is_public = 0
                    url = mes['url']
                    content = '# %s\n**转载**\n文章地址：%s\n<iframe sandbox="allow-forms allow-scripts allow-same-origin" src="%s"></iframe>' % (title, url, url)
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
                        raise
                except Exception as e:
                    _LOGGER.debug('XX: 失败，消息确认失败: %s，错误信息: %s', str(message), str(e))
            with LOCK:
                SIG += 1

SIG = MINGMQ_CONFIG['add_article']['pool_size']
LOCK = Lock()


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER, SIG
    _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['add_article']['queue_name'],))


    while True:
        if SIG > 0:
            try:
                mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
                if mq_res is None:
                    _LOGGER.debug('服务器意外关闭')
                    sys.exit(1)
                if mq_res and mq_res['status'] == FAIL:
                    raise Exception("任务队列中没有任务")
                _LOGGER.debug('从消息队列中获取的消息为: %s', mq_res)
            except Exception as e:
                _LOGGER.debug('XX: 从消息队列中获取任务失败，错误信息: %s', str(e))
                time.sleep(3)
                continue
            try:
                Thread(target=_task, args=(mq_res, queue_name)).start()
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
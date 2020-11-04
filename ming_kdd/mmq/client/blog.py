import json
import logging
import traceback
from threading import Thread, Lock
import time
import sys

from ming_kdd.mmq.settings.mq_cli import MINGMQ_CONFIG
from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG as SERV_MC
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.blog import ranyifeng, wangyin


_MINGMQ_POOL = None

_LOGGER = logging.getLogger('blog_get_article_category_consumer')


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


def _task(mq_res, queue_name, lock, sig):
    with lock:
        sig -= 1

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            category_id: int = message_data['category_id']
            if category_id == 40:  # 王垠
                # [{'title': xxx, 'url': xxx}]
                message = wangyin.pag_article_list()
                _LOGGER.debug('抓取到王垠的数据为: %s', str(message))
                mq_res1 = _MINGMQ_POOL.opera('send_data_to_queue', *(SERV_MC['add_article']['queue_name'], json.dumps({
                    'category_id': category_id,
                    'message': message
                })))
                _LOGGER.debug('推送到消息队列的数据为: %s', str({
                    'category_id': category_id,
                    'message': message
                }))

                if mq_res1 and mq_res1['status'] != FAIL:
                    b = True
            elif category_id == 39:  # 阮一峰
                message = []
                for url in ranyifeng.get_categories():
                    message += ranyifeng.pag_article_list(url)
                    time.sleep(5)
                _LOGGER.debug('抓取到阮一峰的数据为: %s', str(message))
                mq_res1 = _MINGMQ_POOL.opera('send_data_to_queue', *(SERV_MC['add_article']['queue_name'], json.dumps({
                    'category_id': category_id,
                    'message': message
                })))
                _LOGGER.debug('推送到消息队列的数据为: %s', str({
                    'category_id': category_id,
                    'message': message
                }))
                if mq_res1 and mq_res1['status'] != FAIL:
                    b = True
            else:
                raise Exception('不合法的category_id')
        except Exception as e:
            _LOGGER.debug('XX: 失败，推送到消息队列的数据为: %s，错误信息: %s', str(mq_res), str(e))
        finally:
            if b == True:
                message_id = mq_res['json_obj'][0]['message_id']
                try:
                    mq_res = _MINGMQ_POOL.opera('ack_message', *(queue_name, message_id))
                    if mq_res and mq_res['status'] != FAIL:
                        _LOGGER.debug('消息确认成功')
                    raise Exception()
                except Exception as e:
                    _LOGGER.debug('XX: 失败，消息确认失败: %s，错误信息: %s，队列: %s', str(message_id), str(e), queue_name)
            with lock:
                sig += 1


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER
    _MINGMQ_POOL.opera('declare_queue', *(queue_name,))
    _MINGMQ_POOL.opera('declare_queue', *(SERV_MC['add_article']['queue_name'],))

    sig = MINGMQ_CONFIG['get_article_category']['pool_size'] - 1
    lock = Lock()

    while True:
        if sig != 0:
            try:
                mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
                if mq_res is None:
                    _LOGGER.error('服务器意外关闭')
                    sys.exit(1)
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
        _init_mingmq_pool()
        # 测试
        _MINGMQ_POOL.opera('send_data_to_queue', *(MINGMQ_CONFIG['get_article_category']['queue_name'], json.dumps({
            "category_id": 40
        })))

        _MINGMQ_POOL.opera('send_data_to_queue', *(MINGMQ_CONFIG['get_article_category']['queue_name'], json.dumps({
            "category_id": 39
        })))

        _get_data_from_queue(MINGMQ_CONFIG['get_article_category']['queue_name'])
    except:
        _LOGGER.error(traceback.format_exc())
        try:
            _release_mingmq_pool()
        except:
            _LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
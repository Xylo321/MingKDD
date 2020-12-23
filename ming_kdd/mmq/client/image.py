"""
图片抓取逻辑

从ming_kdd_get_photo_website队列中获取任务
如:
{
    "wibsite": "http://www.9610.com/map.htm"
}

完成任务之后，将数据推向队列ming_kdd_add_photo
如:
{
    "website": "http://www.9610.com/map.htm",
    "message":
    [
        "shufa_tag": "书法欣赏",
        "shufas": [
            {
            "name": "王羲之",
            "url": "xxx"
            },
        ]
    ]
}
"""

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
from ming_kdd.image.shufakongjian import get_categories, get_photos

_MINGMQ_POOL = None

_LOGGER = logging.getLogger('photo_get_photo_category_consumer')


def _init_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['get_photo_website']['host'],
                              MINGMQ_CONFIG['get_photo_website']['port'],
                              MINGMQ_CONFIG['get_photo_website']['user_name'],
                              MINGMQ_CONFIG['get_photo_website']['passwd'],
                              MINGMQ_CONFIG['get_photo_website']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()



def _photos():
    cats = get_categories()
    for cat in cats:
        yield {
            "shufa_tag": cat['name'],
            "shufas": get_photos(cat['url'])
        }


def _task(mq_res, queue_name):
    global SIG, LOCK
    with LOCK:
        SIG -= 1

    if mq_res and mq_res['status'] != FAIL and len(mq_res['json_obj']) != 0:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            website: str = message_data['website']
            if website == "http://www.9610.com/map.htm": # 书法空间
                for photo_dict in _photos():
                    mq_res1 = _MINGMQ_POOL.opera('send_data_to_queue', *(SERV_MC['add_photo']['queue_name'], json.dumps({
                        'website': website,
                        'message': [photo_dict]
                    })))
                    _LOGGER.info('推送到消息队列的数据为: %s', str({
                        'website': website,
                        'message': [photo_dict]
                    }))

                    if mq_res1 and mq_res1['status'] == FAIL:
                        raise Exception('推送失败: %s' % str(mq_res1))
            else:
                raise Exception('不合法的website')
        except Exception as e:
            _LOGGER.info('XX: 失败，推送到消息队列的数据为: %s，错误信息: %s', str(e), traceback.format_exc())
        finally:
            if b == True:
                message_id = mq_res['json_obj'][0]['message_id']
                try:
                    mq_res = _MINGMQ_POOL.opera('ack_message', *(queue_name, message_id))
                    if mq_res and mq_res['status'] != FAIL:
                        _LOGGER.info('消息确认成功')
                    else:
                        raise Exception()
                except Exception as e:
                    _LOGGER.info('XX: 失败，消息确认失败: %s，错误信息: %s，队列: %s', str(message_id), str(e), queue_name)
            with LOCK:
                SIG += 1

SIG = sig = MINGMQ_CONFIG['get_photo_website']['pool_size']
LOCK = Lock()

def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER, SIG, Lock

    while True:
        if SIG > 0:
            mq_res = None
            try:
                mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
                if mq_res and mq_res['status'] == FAIL:
                    raise Exception("任务队列中没有任务")
                if mq_res is None:
                    _LOGGER.debug('服务器内部错误')
                    sys.exit(1)
            except Exception as e:
                _LOGGER.info('XX: 从消息队列中获取任务失败，错误信息: %s', str(e))
            try:
                if mq_res and mq_res['status'] != FAIL:
                    _LOGGER.info('从消息队列中获取的消息为: %s', mq_res)
                    Thread(target=_task, args=(mq_res, queue_name)).start()
            except Exception as e:
                _LOGGER.info("XX: 线程在执行过程中出现异常，错误信息为: %s", str(e))
        time.sleep(10)


def main(info=logging.INFO) -> None:
    logging.basicConfig(level=info)
    global _LOGGER
    try:
        _init_mingmq_pool()
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['get_photo_website']['queue_name'],))
        _MINGMQ_POOL.opera('declare_queue', *(SERV_MC['add_photo']['queue_name'],))

        # 测试
        _MINGMQ_POOL.opera('send_data_to_queue', *(MINGMQ_CONFIG['get_photo_website']['queue_name'], json.dumps({
            "website": "http://www.9610.com/map.htm"
        })))

        _get_data_from_queue(MINGMQ_CONFIG['get_photo_website']['queue_name'])
    except:
        _LOGGER.error(traceback.format_exc())
        try:
            _release_mingmq_pool()
        except:
            _LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
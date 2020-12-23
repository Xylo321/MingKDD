import logging
import traceback
from threading import Thread, Lock, get_ident, active_count
import time
import json
import sys

from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG, DATA_TYPE, URL_TYPE
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.mmq.settings.db import IMAGE_MYSQL_CONFIG
from ming_kdd.mmq.db.image import Image, DataSrc, Category
from ming_kdd.mmq.db.rdbms import MySQLPool


_IMAGE_MYSQL_POOL = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('photo_add_photo_consumer')


def _init_mysql_pool() -> None:
    global _IMAGE_MYSQL_POOL
    _IMAGE_MYSQL_POOL = MySQLPool(host=IMAGE_MYSQL_CONFIG['host'],
                                  user=IMAGE_MYSQL_CONFIG['user'],
                                  passwd=IMAGE_MYSQL_CONFIG['passwd'],
                                  db=IMAGE_MYSQL_CONFIG['db'],
                                  size=IMAGE_MYSQL_CONFIG['size'])

def _release_mysql_pool():
    global _IMAGE_MYSQL_POOL
    _IMAGE_MYSQL_POOL.release()

def _init_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['add_photo']['host'],
                              MINGMQ_CONFIG['add_photo']['port'],
                              MINGMQ_CONFIG['add_photo']['user_name'],
                              MINGMQ_CONFIG['add_photo']['passwd'],
                              MINGMQ_CONFIG['add_photo']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()


def _shufakongjian(website, message):
    """处理数据

    message例:
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
    data_src = DataSrc(_IMAGE_MYSQL_POOL)
    photo = Image(_IMAGE_MYSQL_POOL)
    category = Category(_IMAGE_MYSQL_POOL)
    data_src_id = data_src.get_id_by_web_site(website)
    if data_src_id is None:
        raise Exception('website不合法: %s', website)

    for shufa in message:
        shufa_tag = shufa['shufa_tag']
        category_id = category.get_category_id_by_name(shufa_tag)
        if category_id is None:
            category.add_category(shufa_tag)
            category_id = category.get_category_id_by_name(shufa_tag)
            if category_id is None:
                raise Exception('插入图片分类失败:%s ', shufa_tag)

        shufas = shufa['shufas']
        for sf in shufas:
            title = sf['name']
            file_extension = ''
            try:
                file_extension = sf['url'].rsplit('.', 1)[1]
            except:
                _LOGGER.debug('url中没有包含扩展名:%s', sf['url'])
            date = int(time.time())
            photo_id = photo.get_photo_id(title)
            if photo_id is None:
                photo.add_photo(title, category_id, file_extension, date, data_src_id)
                photo_id = photo.get_photo_id(title)

            downloaded = photo.get_photo_download_status(photo_id)
            if downloaded != None and downloaded == 1:
                # 图片已经下载
                return

            url = sf['url']
            data_type = DATA_TYPE['photo']
            url_type = URL_TYPE['直接下载']
            task = json.dumps({
                'id': photo_id,
                'category_id': category_id,
                'title': title,
                'url': url,
                'data_type': data_type,
                'url_type': url_type,
                'file_extension': file_extension
            })
            mq_res1 = _MINGMQ_POOL.opera('send_data_to_queue', *(MINGMQ_CONFIG['download']['queue_name'], task))
            if mq_res1 and mq_res1['status'] == FAIL:
                raise Exception('下载任务未推送到队列: %s', title)


def _task(mq_res, queue_name):
    global _IMAGE_MYSQL_POOL, SIG, LOCK

    with LOCK: SIG -= 1

    _LOGGER.debug('当前线程: %s，总线程数: %d, sig: %d', get_ident(), active_count(), SIG)

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            website: str = message_data['website']
            message: list = message_data['message']
            if website == 'http://www.9610.com/map.htm':
                _shufakongjian(website, message)
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
            with LOCK:
                SIG += 1

SIG = MINGMQ_CONFIG['add_photo']['pool_size']
LOCK = Lock()


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER

    while True:
        if SIG > 0:
            try:
                mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
                if mq_res and mq_res['status'] == FAIL:
                    raise Exception("任务队列中没有任务")
                if mq_res is None:
                    _LOGGER.error("服务器内部错误")
                    sys.exit(1)
                _LOGGER.debug('从消息队列中获取的消息为: %s', mq_res)
            except Exception as e:
                _LOGGER.debug('XX: 从消息队列中获取任务失败，错误信息: %s', str(e))
                time.sleep(10)

                continue
            try:
                Thread(target=_task, args=(mq_res, queue_name)).start()
            except Exception as e:
                _LOGGER.debug("XX: 线程在执行过程中出现异常，错误信息为: %s", str(e))
            time.sleep(1)
        else:
            time.sleep(10)

def main(debug=logging.DEBUG) -> None:
    logging.basicConfig(level=debug)
    global _LOGGER
    try:
        _init_mysql_pool()
        _init_mingmq_pool()
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['add_photo']['queue_name'],))
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['download']['queue_name'],))

        _get_data_from_queue(MINGMQ_CONFIG['add_photo']['queue_name'])
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
import logging
import traceback
from threading import Thread, Lock
import time
import json
import os

from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG, DATA_TYPE, URL_TYPE
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.mmq.settings.db import VIDEO_MYSQL_CONFIG
from ming_kdd.mmq.db.video import Video, DataSrc, Category
from ming_kdd.mmq.db.rdbms import MySQLPool
from ming_kdd.utils.m3u8.m3u8 import download_m3u8_video
from ming_kdd.utils.mdfs import upload
from ming_kdd.mmq.settings.dfs import MDFS_API_KEY, MDFS_UPLOAD_URL
from ming_kdd.mmq.settings.db import ROBOT


_VIDEO_MYSQL_POOL = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('blog_download_consumer')


def _init_mysql_pool() -> None:
    global _VIDEO_MYSQL_POOL
    _VIDEO_MYSQL_POOL = MySQLPool(host=VIDEO_MYSQL_CONFIG['host'],
                                  user=VIDEO_MYSQL_CONFIG['user'],
                                  passwd=VIDEO_MYSQL_CONFIG['passwd'],
                                  db=VIDEO_MYSQL_CONFIG['db'],
                                  size=VIDEO_MYSQL_CONFIG['size'])

def _release_mysql_pool():
    global _VIDEO_MYSQL_POOL
    _VIDEO_MYSQL_POOL.release()

def _init_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['download']['host'],
                              MINGMQ_CONFIG['download']['port'],
                              MINGMQ_CONFIG['download']['user_name'],
                              MINGMQ_CONFIG['download']['passwd'],
                              MINGMQ_CONFIG['download']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()


def _download_m3u8(data_id, category_id, url, title, data_type):
    if download_m3u8_video([url], [title]):
        if upload(MDFS_UPLOAD_URL, MDFS_API_KEY, ROBOT, category_id, title, title, 'mp4') == 1:
            os.remove(title)

            video = Video(_VIDEO_MYSQL_POOL)
            if data_type == DATA_TYPE['video']:
                video.change_video_download_status(data_id)

def _task(mq_res, queue_name, lock, sig):
    global _VIDEO_MYSQL_POOL

    with lock:
        sig -= 1

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            """
            'id': video_id,
            'category_id': category_id
            'url': m3url,
            'data_type': data_type,
            'url_type': url_type
            """
            data_id = message_data['id']
            category_id = message_data['category_id']
            url = message_data['url']
            data_type: list = message_data['data_type']
            url_type: list = message_data['url_type']

            if url_type == URL_TYPE['直接下载']:
                pass
            elif url_type == URL_TYPE['m3url']:
                _download_m3u8(data_id, category_id, url, data_type)
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
                    _LOGGER.debug('XX: 失败，消息确认失败: %s，错误信息: %s', str(message_data), str(e))
            with lock:
                sig += 1


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER

    sig = MINGMQ_CONFIG['download']['pool_size']
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
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['download']['queue_name'],))

        _get_data_from_queue(MINGMQ_CONFIG['download']['queue_name'])
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
import logging
import traceback
from threading import Thread, Lock, active_count, get_ident
import time
import json
import os
import sys
import requests

from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG, DATA_TYPE, URL_TYPE
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.mmq.settings.db import VIDEO_MYSQL_CONFIG, IMAGE_MYSQL_CONFIG
from ming_kdd.mmq.db.video import Video
from ming_kdd.mmq.db.image import Image
from ming_kdd.mmq.db.rdbms import MySQLPool
from ming_kdd.utils.m3u8.m3u8 import download_m3u8_video
from ming_kdd.utils.mdfs import upload
from ming_kdd.mmq.settings.dfs import MDFS_API_KEY, MDFS_UPLOAD_URL
from ming_kdd.mmq.settings.db import ROBOT


_IMAGE_MYSQL_POOL = None
_VIDEO_MYSQL_POOL = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('download_consumer')


def _init_mysql_pool() -> None:
    global _VIDEO_MYSQL_POOL, _IMAGE_MYSQL_POOL
    _VIDEO_MYSQL_POOL = MySQLPool(host=VIDEO_MYSQL_CONFIG['host'],
                                  user=VIDEO_MYSQL_CONFIG['user'],
                                  passwd=VIDEO_MYSQL_CONFIG['passwd'],
                                  db=VIDEO_MYSQL_CONFIG['db'],
                                  size=VIDEO_MYSQL_CONFIG['size'])
    _IMAGE_MYSQL_POOL = MySQLPool(host=IMAGE_MYSQL_CONFIG['host'],
                                  user=IMAGE_MYSQL_CONFIG['user'],
                                  passwd=IMAGE_MYSQL_CONFIG['passwd'],
                                  db=IMAGE_MYSQL_CONFIG['db'],
                                  size=IMAGE_MYSQL_CONFIG['size'])

def _release_mysql_pool():
    global _VIDEO_MYSQL_POOL, _IMAGE_MYSQL_POOL
    _VIDEO_MYSQL_POOL.release()
    _IMAGE_MYSQL_POOL.release()


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


def _download_m3u8(data_id, category_id, url, title, data_type, file_extension):
    if download_m3u8_video([url], [title]):
        file_name = file_path = '%s.%s' % (title, file_extension)
        if upload(MDFS_UPLOAD_URL, MDFS_API_KEY, ROBOT, category_id, title, file_name, file_path) == 1:
            try:
                os.remove(file_path)
            except:
                _LOGGER.error(traceback.format_exc())

            _change_download_status(data_type, data_id)
        else:
            raise Exception('上传失败: %s' % title)
    else:
        raise Exception('下载失败: %s' % title)


def _data_downloaded(data_type, data_id):
    video = Video(_VIDEO_MYSQL_POOL)
    image = Image(_IMAGE_MYSQL_POOL)
    downloaded = None
    if data_type == DATA_TYPE['video']:
        downloaded = video.get_video_download_status(data_id)
    elif data_type == DATA_TYPE['photo']:
        downloaded = image.get_photo_download_status(data_id)
    else:
        raise Exception('XX: 不合法的数据类型: %s', data_type)
    if downloaded == 1:
        return True
    elif downloaded is None:
        raise Exception('XX: downloaded is None: %s', str(data_id))
    else:
        return False

def _change_download_status(data_type, data_id):
    video = Video(_VIDEO_MYSQL_POOL)
    image = Image(_IMAGE_MYSQL_POOL)
    if data_type == DATA_TYPE['video']:
        video.change_video_download_status(data_id)
    elif data_type == DATA_TYPE['photo']:
        image.change_photo_download_status(data_id)
    else:
        raise Exception('XX: 不合法的数据类型: %s', data_type)

def _download_photo(data_id, category_id, url, title, data_type, file_extension):
    file_path = filename = '%s.%s' % (str(time.time()), file_extension)

    try:
        r = requests.get(url, stream=True)
        chunk_size = 4096
        with open(filename, 'wb') as fd:
            for chunk in r.iter_content(chunk_size):
                fd.write(chunk)
    except:
        # 下载失败
        _LOGGER.error(traceback.format_exc())
        raise Exception('下载失败: %s', title)
    err = 0
    try:
        if upload(MDFS_UPLOAD_URL, MDFS_API_KEY, ROBOT, category_id, title, filename, file_path) == 1:
            _change_download_status(data_type, data_id)
        else:
            err = 1
    except:
        _LOGGER.error(traceback.format_exc())
    finally:
        try:
            os.remove(file_path)
        except:
            _LOGGER.error(traceback.format_exc())

    if err != 0:
        raise Exception('上传失败: %s', title)

def _task(mq_res, queue_name):
    global _VIDEO_MYSQL_POOL, SIG, LOCK
    _LOGGER.debug('当前线程的id为:%d，总线程数:%d', get_ident(), active_count())

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            """
            'id': video_id,
            'category_id': category_id
            'url': m3url,
            'data_type': data_type,
            'url_type': url_type,
            'file_extension': file_extension
            """
            data_id = message_data['id']
            category_id = message_data['category_id']
            url = message_data['url']
            title = message_data['title']
            data_type = message_data['data_type']
            url_type = message_data['url_type']
            file_extension = message_data['file_extension']

            downloaded_status = _data_downloaded(data_type, data_id)
            if downloaded_status == False:
                if data_type == DATA_TYPE['photo']:
                    if url_type == URL_TYPE['直接下载']:
                        _download_photo(data_id, category_id, url, title, data_type, file_extension)
                    elif url_type == URL_TYPE['m3url']:
                        pass
                    else:
                        _LOGGER.error('XX: 不是合法的url类型: %s', str(message_data))
                elif data_type == DATA_TYPE['video']:
                    if url_type == URL_TYPE['直接下载']:
                        pass
                    elif url_type == URL_TYPE['m3url']:
                        _download_m3u8(data_id, category_id, url, title, data_type, file_extension)
                    else:
                        _LOGGER.error('XX: 不是合法的url类型: %s', str(message_data))
                else:
                    _LOGGER.error('XX: 不是合法的数据类型: %s', str(message_data))
            else:
                _LOGGER.info('数据已下载')
            b = True
        except Exception as e:
            _LOGGER.debug('XX: 失败，数据存储到mysql: %s，错误信息: %s，堆栈报错: %s', str(mq_res), str(e), traceback.format_exc())
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
            with LOCK:
                SIG += 1

SIG = MINGMQ_CONFIG['download']['pool_size']
LOCK = Lock()


def _get_data_from_queue(queue_name):
    global _MINGMQ_POOL, _LOGGER, SIG, LOCK

    while True:
        if SIG > 0:
            try:
                mq_res: dict = _MINGMQ_POOL.opera('get_data_from_queue', *(queue_name, ))
                if mq_res and mq_res['status'] == FAIL:
                    raise Exception("任务队列中没有任务")
                if mq_res is None:
                    _LOGGER.debug('服务器内部错误')
                    sys.exit(1)
                _LOGGER.debug('从消息队列中获取的消息为: %s', mq_res)
            except Exception as e:
                _LOGGER.debug('XX: 从消息队列中获取任务失败，错误信息: %s', str(e))
                time.sleep(3)
                continue
            try:
                with LOCK: SIG -= 1
                Thread(target=_task, args=(mq_res, queue_name)).start()
            except Exception as e:
                _LOGGER.debug("XX: 线程在执行过程中出现异常，错误信息为: %s", str(e))
        else:
            time.sleep(3)


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
import logging
import traceback
from threading import Thread, Lock, get_ident, active_count
import time
import json
import sys

from ming_kdd.mmq.settings.mq_serv import MINGMQ_CONFIG, DATA_TYPE, URL_TYPE
from mingmq.client import Pool as MingMQPool
from mingmq.message import FAIL
from ming_kdd.mmq.settings.db import VIDEO_MYSQL_CONFIG
from ming_kdd.mmq.db.video import Video, DataSrc, Category
from ming_kdd.mmq.db.rdbms import MySQLPool


_VIDEO_MYSQL_POOL = None
_MINGMQ_POOL = None

_LOGGER = logging.getLogger('video_add_video_consumer')


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
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['add_video']['host'],
                              MINGMQ_CONFIG['add_video']['port'],
                              MINGMQ_CONFIG['add_video']['user_name'],
                              MINGMQ_CONFIG['add_video']['passwd'],
                              MINGMQ_CONFIG['add_video']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()


def _m3url_juji(website, message):
    """处理数据

    message例:
    {
        "website": "https://www.hanjutv.com/",
        "message": json.dumps(
            [
                {
                    "ju_name": "九尾狐传",
                    "jujis": [
                        {
                            "m3url": xxx,
                            "file_name": xxx
                        },
                        {
                            "m3url": xxx,
                            "file_name": xxx
                        }
                    ]
                },
            ]
        )
    }
    """
    data_src = DataSrc(_VIDEO_MYSQL_POOL)
    video = Video(_VIDEO_MYSQL_POOL)
    category = Category(_VIDEO_MYSQL_POOL)
    data_src_id = data_src.get_id_by_web_site(website)
    if data_src_id is None:
        raise Exception('website不合法: %s', website)

    for ju in message:
        ju_name = ju['ju_name']
        category_id = category.get_category_id_by_name(ju_name)
        if category_id is None:
            category.add_category(ju_name)
            category_id = category.get_category_id_by_name(ju_name)
            if category_id is None:
                raise Exception('插入电影分类失败:%s ', ju_name)

        jujis = ju['jujis']
        for juji in jujis:
            title = juji['file_name']
            file_extension = 'mp4'
            date = int(time.time())
            description = ''
            video_id = video.get_video_id(title)
            if video_id is None:
                video.add_video(title, category_id, file_extension, date, description, data_src_id)
                video_id = video.get_video_id(title)

            downloaded = video.get_video_download_status(video_id)
            if downloaded != None and downloaded == 1:
                # 视频已经下载
                return

            m3url = juji['m3url']
            # 建议这里最好分发下载，不然，太多文件要下载，这个任务要猴年马月才能完成
            data_type = DATA_TYPE['video']
            url_type = URL_TYPE['m3url']
            task = json.dumps({
                'id': video_id,
                'category_id': category_id,
                'title': title,
                'url': m3url,
                'data_type': data_type,
                'url_type': url_type
            })
            mq_res1 = _MINGMQ_POOL.opera('send_data_to_queue', *(MINGMQ_CONFIG['download']['queue_name'], task))
            if mq_res1 and mq_res1['status'] == FAIL:
                raise Exception('下载任务未推送到队列: %s', title)


def _task(mq_res, queue_name):
    global _VIDEO_MYSQL_POOL, SIG, LOCK

    with LOCK: SIG -= 1

    _LOGGER.debug('当前线程: %s，总线程数: %d, sig: %d', get_ident(), active_count(), SIG)

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            website: str = message_data['website']
            message: list = message_data['message']
            if website == 'https://www.hanjutv.com/':
                _m3url_juji(website, message)
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

SIG = MINGMQ_CONFIG['add_video']['pool_size']
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
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['add_video']['queue_name'],))
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['download']['queue_name'],))

        _get_data_from_queue(MINGMQ_CONFIG['add_video']['queue_name'])
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
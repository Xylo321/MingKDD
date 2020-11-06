"""
电影抓取逻辑

从ming_kdd_get_video_website队列中获取任务
如:
{
    "wibsite": "https://www.hanjutv.com/"
}

完成任务之后，将数据推向队列ming_kdd_add_video
如:
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
from ming_kdd.video import hanjutv


_MINGMQ_POOL = None

_LOGGER = logging.getLogger('video_get_video_category_consumer')


def _init_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL = MingMQPool(MINGMQ_CONFIG['get_video_website']['host'],
                              MINGMQ_CONFIG['get_video_website']['port'],
                              MINGMQ_CONFIG['get_video_website']['user_name'],
                              MINGMQ_CONFIG['get_video_website']['passwd'],
                              MINGMQ_CONFIG['get_video_website']['pool_size'])


def _release_mingmq_pool() -> None:
    global _MINGMQ_POOL
    _MINGMQ_POOL.release()


def _hanju():
    # 第一阶段
    hanjus = []
    last_page = 0
    for i in range(3): # 重试3次，3次失败则退出
        try:
            hanjus, last_page = hanjutv.get_hanjus()
            break
        except:
            _LOGGER.error('获取韩剧第一页失败')
    if len(hanjus) == 0 or last_page == 0:
        raise Exception("获取第一页失败")

    # 第二阶段
    for page in range(1, last_page + 1):
        if page != 1:
            try:
                hanjus, last_page = hanjutv.get_hanjus(page)
            except Exception as e:
                _LOGGER.error('分页面时失败，进入下一页: %s', str(e))
                continue
        for hanju in hanjus:
            hanju_name = hanju['title']
            video_dict = {
                "ju_name": hanju_name,
                "jujis": []
            }
            try:  # 这个try是为了爬取某个电视剧的剧集urls失败时，继续下一个电视剧
                for juji in hanjutv.get_hanju_jujis(hanju['url']):
                    try:  # 这个try是为了爬取某一集失败时，继续下一个集数爬取
                        m3url = hanjutv.get_m3u8(juji[0])
                        file_name = hanju['title'] + f'(第{juji[1]}集)'
                        _LOGGER.info("爬取的剧集信息为: %s，%s", file_name, m3url)
                        video_dict['jujis'].append({
                            'file_name': file_name,
                            'm3url': m3url
                        })
                    except:
                        _LOGGER.error("获取剧集m3ul失败: %s", str(juji))
                    # finally:
                        # time.sleep(5)
            except:
                _LOGGER.error("爬取剧集时失败: %s", str(hanju))
            finally:
                yield video_dict


def _task(mq_res, queue_name):
    global SIG, LOCK
    with LOCK:
        SIG -= 1

    if mq_res and mq_res['status'] != FAIL:
        b = False
        try:
            message_data = json.loads(mq_res['json_obj'][0]['message_data'])
            website: str = message_data['website']
            if website == "https://www.hanjutv.com/":  # 韩剧TV
                for video_dict in _hanju():
                    mq_res1 = _MINGMQ_POOL.opera('send_data_to_queue', *(SERV_MC['add_video']['queue_name'], json.dumps({
                        'website': website,
                        'message': [video_dict]
                    })))
                    _LOGGER.info('推送到消息队列的数据为: %s', str({
                        'website': website,
                        'message': [video_dict]
                    }))

                    if mq_res1 and mq_res1['status'] == FAIL:
                        raise Exception('推送失败: %s' % str(mq_res1))
            else:
                raise Exception('不合法的website')
        except Exception as e:
            _LOGGER.info('XX: 失败，推送到消息队列的数据为: %s，错误信息: %s', str(mq_res), str(e))
        finally:
            if b == True:
                message_id = mq_res['json_obj'][0]['message_id']
                try:
                    mq_res = _MINGMQ_POOL.opera('ack_message', *(queue_name, message_id))
                    if mq_res and mq_res['status'] != FAIL:
                        _LOGGER.info('消息确认成功')
                    raise Exception()
                except Exception as e:
                    _LOGGER.info('XX: 失败，消息确认失败: %s，错误信息: %s，队列: %s', str(message_id), str(e), queue_name)
            with LOCK:
                SIG += 1

SIG = sig = MINGMQ_CONFIG['get_video_website']['pool_size']
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
        _MINGMQ_POOL.opera('declare_queue', *(MINGMQ_CONFIG['get_video_website']['queue_name'],))
        _MINGMQ_POOL.opera('declare_queue', *(SERV_MC['add_video']['queue_name'],))

        # 测试
        _MINGMQ_POOL.opera('send_data_to_queue', *(MINGMQ_CONFIG['get_video_website']['queue_name'], json.dumps({
            "website": "https://www.hanjutv.com/"
        })))

        _get_data_from_queue(MINGMQ_CONFIG['get_video_website']['queue_name'])
    except:
        _LOGGER.error(traceback.format_exc())
        try:
            _release_mingmq_pool()
        except:
            _LOGGER.error(traceback.format_exc())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    main()
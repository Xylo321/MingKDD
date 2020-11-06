"""
https://www.hanjutv.com/v_all/list-catid-7.html 第一页
https://www.hanjutv.com/v_all/list-catid-7-page-1.html 第一页
https://www.hanjutv.com/v_all/list-catid-7-page-44.html 第44页

夫妻的世界
https://meiju11.zzwc120.com/20200328/U08z7Ea8/2000kb/hls/index.m3u8?wsSecret=6aea2b4b915bc2fdba926dc0e46baf95&wsTime=1602522797

https://meiju11.zzwc120.com/20200328/U08z7Ea8/2000kb/hls/cKV1nNzk.js
https://meiju11.zzwc120.com/20200328/U08z7Ea8/2000kb/hls/zsvWwkxg.js
"""

import requests
from lxml import etree
import os
import shutil
import traceback
from ming_kdd.utils.m3u8.m3u8 import download_m3u8_video
import logging


ROOT_URL = 'https://www.hanjutv.com'


def get_hanjus(page=1):
    """获取韩剧列表，分页获取。返回韩剧列表，和最后页数。第一次若发生意外则抛出异常
    """
    if page < 1: raise
    # .m-item > .thumb
    # .mod-pagination > ul > li > a
    url_template = 'https://www.hanjutv.com/v_all/list-catid-7-page-%d.html'
    r = requests.get(url_template % page)

    body_dom = etree.HTML(r.text)
    hanju_doms = body_dom.cssselect('.m-item > .thumb')

    last_page = -1
    if page == 1:
        last_page = body_dom.cssselect('.mod-pagination > ul > li > a')[-1].get('data-ci-pagination-page')
    if page == 1 and last_page is None: raise
    return [{"title": hanju_dom.get('title'), "url": ROOT_URL + hanju_dom.get('href')} for hanju_dom in hanju_doms], int(last_page)


def get_hanju_jujis(hanju_url):
    """获取剧集
    """
    global ROOT_URL
    # .detail-main-btn > .hjtvui-btn
    r = requests.get(hanju_url)
    r.encoding = 'utf-8'
    body_dom = etree.HTML(r.text)
    juji_doms = body_dom.cssselect('.juji-list > li > a')
    jujis = []
    if len(juji_doms) == 0:
        go_paly_url = ROOT_URL + body_dom.cssselect('.detail-main-btn > .hjtvui-btn')[0].get('href')
        # .playBox > #playPath
        jujis.append([go_paly_url, '1'])
    else:
        for juji_dom in juji_doms:
            juji_name = juji_dom.text
            if juji_name == '播放': juji_name = '1'
            jujis.append([ROOT_URL + juji_dom.get('href'), juji_name])
    return jujis


def get_m3u8(go_play_url):
    """获取m3u8清单文件地址
    """
    r = requests.get(go_play_url)
    body_dom = etree.HTML(r.text)
    play_url = 'http:' + body_dom.cssselect('.playBox > #playPath')[0].get('src')
    logging.debug('play_url: %s', play_url)

    # r = requests.get(play_url)
    #
    # print(r.text)
    # for line in r.text.splitlines():
    #     if 'url:' in line:
    #         result = line.split('url:')[1].replace("'", "").replace(' ', '')
    #         # result = result.split('?')[0]
    #         logging.debug('获取的m3url: %s', result)
    #         return result
    return play_url.split('?path=')[1]


def _task(hanju_path, hanju, juji):
    m3url = get_m3u8(juji[0])
    file_name = hanju['title'] + f'(第{juji[1]}集)'
    if not os.path.exists(hanju_path + os.path.sep + file_name + '.mp4'):
        print('Downloading...', hanju_path + os.path.sep + juji[1] + '.mp4')
        download_m3u8_video([m3url, ], [file_name, ])
        shutil.move(os.getcwd() + os.path.sep + file_name + '.mp4', hanju_path)
        print('Download Completely.', file_name)


def download_all(root_path='../download/韩剧TV/'):
    """整站下载所有韩剧
    """
    if not os.path.exists(root_path): os.makedirs(root_path)

    hanjus, last_page = get_hanjus()

    for page in range(1, last_page + 1):
        if page != 1:
            hanjus, last_page = get_hanjus(page)
        print(hanjus)
        try:
            for hanju in hanjus:
                hanju_path = root_path + os.path.sep + hanju['title']
                if not os.path.exists(hanju_path):
                    os.mkdir(hanju_path)
                try:
                    for juji in get_hanju_jujis(hanju['url']):
                        _task(hanju_path, hanju, juji)
                except:
                    print(traceback.format_exc())
        except:
            print(traceback.format_exc())


def download_one(juji_url, hanju_title, root_path='../download/韩剧TV/'):
    """指定名的韩剧下载
    """
    if not os.path.exists(root_path): os.makedirs(root_path)

    hanju_path = root_path + os.path.sep + hanju_title
    if not os.path.exists(hanju_path):
        os.mkdir(hanju_path)
    try:
        for juji in get_hanju_jujis(juji_url):
            _task(hanju_path, {'title': hanju_title}, juji)
    except: pass


if __name__ == '__main__':
    # download_all()
    download_one('https://www.hanjutv.com/hanju/2406.html', '天国的阶梯')
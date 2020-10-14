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

    r = requests.get(play_url)

    for line in r.text.splitlines():
        if 'url:' in line:
            return line.split('url:')[1].replace("'", "").replace(' ', '')
    raise


if __name__ == '__main__':
    import os
    import shutil
    from reborn_kdd.utils.m3u8.m3u8 import download_m3u8_video

    root_path = '../../download/'
    hanjus, last_page = get_hanjus()
    print(hanjus, last_page)
    for page in range(1, last_page + 1):
        hanjus, last_page = get_hanjus(page)
        print(hanjus)
        try:
            for hanju in hanjus:
                hanju_path = root_path + os.path.sep + hanju['title']
                if not os.path.exists(hanju_path):
                    os.mkdir(hanju_path)
                try:
                    for juji in get_hanju_jujis(hanju['url']):
                        m3url = get_m3u8(juji[0])
                        if not os.path.exists(hanju_path + os.path.sep + juji[1] + '.mp4'):
                            print('Downloading...', hanju_path + os.path.sep + juji[1] + '.mp4')
                            download_m3u8_video([m3url, ], [juji[1], ])
                            shutil.move(os.path.split(os.path.abspath(__file__))[0] + os.path.sep + juji[1] + '.mp4', hanju_path)
                except: pass
        except: pass
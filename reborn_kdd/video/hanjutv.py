"""
https://www.hanjutv.com/v_all/list-catid-7.html 第一页
https://www.hanjutv.com/v_all/list-catid-7-page-1.html 第一页
https://www.hanjutv.com/v_all/list-catid-7-page-44.html 第44页

夫妻的世界
https://meiju11.zzwc120.com/20200328/U08z7Ea8/2000kb/hls/index.m3u8?wsSecret=6aea2b4b915bc2fdba926dc0e46baf95&wsTime=1602522797

https://meiju11.zzwc120.com/20200328/U08z7Ea8/2000kb/hls/cKV1nNzk.js
https://meiju11.zzwc120.com/20200328/U08z7Ea8/2000kb/hls/zsvWwkxg.js

清单文件与网络监控列表是乱序的，意味着必须开发一个demo这种视频网站才能彻底搞清楚内幕。

不急。
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


def get_hanju_m3u8_list(hanju_url):
    """获取该韩剧的m3u8清单文件。
    """
    global ROOT_URL
    # .detail-main-btn > .hjtvui-btn
    r = requests.get(hanju_url)
    body_dom = etree.HTML(r.text)
    go_paly_url = ROOT_URL + body_dom.cssselect('.detail-main-btn > .hjtvui-btn')[0].get('href')
    # .playBox > #playPath
    r = requests.get(go_paly_url)
    body_dom = etree.HTML(r.text)
    play_url = 'http:' + body_dom.cssselect('.playBox > #playPath')[0].get('src')

    r = requests.get(play_url)

    for line in r.text.splitlines():
        if 'url:' in line:
            return line.split('url:')[1].replace("'", "").replace(' ', '')
    raise


if __name__ == '__main__':
    hanjus, last_page = get_hanjus()
    print(hanjus, last_page)
    for page in range(2, last_page + 1):
        hanjus, last_page = get_hanjus(page)
        print(hanjus)
        for hanju in hanjus:
            try:
                m3u8_url = get_hanju_m3u8_list(hanju['url'])
                print(m3u8_url)
            except:
                print('err', hanju)
                import traceback
                print(traceback.format_exc())
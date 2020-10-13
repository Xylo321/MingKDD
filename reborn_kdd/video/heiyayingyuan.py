"""
https://www.hmheiya.com/fen/lunlipian.html
https://www.hmheiya.com/fen/lunlipian-2.html

page-link
>
<a class="page-link" href="#">当前2/29页</a>
"""
import requests
from lxml import etree
import time
from reborn_kdd.google_driver.chrome_driver import ChromeDriver

ROOT_URL = 'https://www.hmheiya.com'
USER_AGENT = 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'

def get_pians(page=1):
    url_template = 'https://www.hmheiya.com/fen/lunlipian-%d.html'
    r = requests.get(url_template % page, headers={'User-Agent': USER_AGENT})
    body_dom = etree.HTML(r.text)
    last_page = int(body_dom.cssselect('.page-link')[-1].text.split('/')[1].replace('页', ''))
    pian_doms = body_dom.cssselect('ul > li > .thumbnail')
    pians = [{'href': ROOT_URL + pian_dom.get('href'), 'title': pian_dom.cssselect('img')[0].get('alt')} for pian_dom in pian_doms]
    return last_page, pians


def get_m3u8(pian_url):
    r = requests.get(pian_url, headers={'User-Agent': USER_AGENT})
    body_dom = etree.HTML(r.text)
    go_play_url = ROOT_URL + body_dom.cssselect('.detail-play-list > li > a')[-1].get('href')

    r = requests.get(go_play_url, headers={
        'User-Agent': USER_AGENT,
        'Host': 'www.hmheiya.com',
        'Pragma': 'no - cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1'
    })
    # cb = ChromeDriver()
    # cb.open(go_play_url, 30)
    # text = cb.get_page_source()
    # # cb.close()
    # time.sleep(1800)
    for line in r.text.splitlines():
        if 'var cms_player' in line:
            # print(line)
            return line.split('{"url":"')[1].split('","copyright')[0]
    raise


if __name__ == '__main__':
    def download_pian(pians):
        try:
            for pian in pians:
                pian_path = root_path + os.path.sep + pian['title'] + '.mp4'
                if not os.path.exists(pian_path):
                    print(pian_path)
                    try:
                        m3u8_url = get_m3u8(pian['href'])
                        print(m3u8_url)
                        download_m3u8_video([m3u8_url], [pian['title']])
                        pian_file_name = os.path.split(os.path.abspath(__file__))[0] + os.path.sep + pian[
                            'title'] + '.mp4'
                        shutil.move(pian_file_name, root_path)
                    except:
                        import traceback
                        print(traceback.format_exc())
        except:
            pass

    import os
    from reborn_kdd.utils.m3u8 import download_m3u8_video
    import shutil
    root_path = '../../download/黑鸭影院'
    if not os.path.exists(root_path): os.mkdir(root_path)
    last_page, pians = get_pians()
    # download_pian(pians)
    try:
        for page in range(2, last_page + 1):
            try:
                last_page, pians = get_pians(page)
                download_pian(pians)
            except: pass
    except: pass
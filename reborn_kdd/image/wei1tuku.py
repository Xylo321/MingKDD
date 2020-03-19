"""
http://www.mmonly.cc
"""

import traceback
from collections import OrderedDict

import requests
from lxml import etree

TAG = 'http://www.mmonly.cc/tag/'
FILTER_KEYWORDS = ['床上', '阴', 'Av', 'aV', '事业线', '制服', 'av', '女优', 'AV', '车展', '乳', '妇', '胸', '欲', '臀', '尤物', '不雅',
                   '比基尼', '性', '情趣', '诱惑', '妖娆', '写真', '人体艺术', '丰满', '健身', '火辣', '非主流', '媚娘', '私房']


# FILTER_KEYWORDS = []

class Categories(object):
    """标签云"""

    def get_all_tags(self):
        headers = {
            "Host": "www.mmonly.cc",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "close"
        }

        hrefs = []

        try:
            r = requests.get(TAG, headers=headers, timeout=50, verify=False)
            r.encoding = 'gb2312'
            dom = etree.HTML(r.text)

            css = '.TagList > a'
            a_doms = dom.cssselect(css)
            for a_dom in a_doms:
                href = 'http://www.mmonly.cc' + a_dom.get('href')
                print(a_dom.text, href)
                hrefs.append(href)
        except:
            print('爬取tag失败')
            print(traceback.print_exc())

        return hrefs

    def main(self):
        for href in self.get_all_tags():
            yield href


class XiangcePagnation(object):
    """
    相册分页
    """

    def __init__(self, match_url):
        self.match_url = match_url
        self.max_page = 1
        self.min_page = 1

    def pagnation(self, page):
        headers = {
            "Host": "www.mmonly.cc",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "close"
        }
        url = self.match_url

        try:
            if page > 1:
                url = self.match_url + "%d.html" % page

            r = requests.get(url, headers=headers, timeout=50, verify=False)
            r.encoding = 'gb2312'
            dom = etree.HTML(r.text)
            if page == 1:
                last_page_dom = dom.xpath("//a[text()='末页']")
                if len(last_page_dom) != 0:
                    self.max_page = int(last_page_dom[0].get('href').replace('.html', '').split('/')[-1])
                else:
                    raise Exception('最后一页解析规则发生改变')

            xiangce_a_doms = dom.cssselect(".title > span > a")

            hrefs = []
            for xad in xiangce_a_doms:
                err = 0
                had_child = xad.cssselect("b")

                for x in FILTER_KEYWORDS:
                    if len(had_child) == 0:
                        if x in xad.text:
                            # print('很俗气的内容标题', xad.text)
                            err += 1
                            break
                    else:
                        if x in had_child[0].text:
                            # print('很俗气的内容标题', xad.text)
                            err += 1
                            break

                if err == 0:
                    hrefs.append(xad.get('href'))

            return hrefs
        except:
            print("失败的url:", url)
            print(traceback.print_exc())

    def main(self):
        count = self.min_page
        while count <= self.max_page:
            xiangce_hrefs = self.pagnation(count)
            if xiangce_hrefs is not None:
                yield xiangce_hrefs
            count += 1


class DetailPagnation(object):
    """
    通用图片详情分页
    """

    def __init__(self, match_url):
        self.match_url = match_url
        self.max_page = 1
        self.min_page = 1

    def detail_pagnation(self, page):
        """
        再第一页中拿到共有多少页，然后根据分页url规则在相册url后面"_数字”，最大为共用多少页
        """
        headers = OrderedDict({
            "Host": "www.mmonly.cc",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "close"
        })
        url = self.match_url
        if page > 1: url = self.match_url.replace(".html", '') + "_" + str(page) + ".html"
        try:
            r = requests.get(url, headers=headers, timeout=50, verify=False)
            r.encoding = 'gb2312'
            dom = etree.HTML(r.text)
            if page == 1:
                # m = re.match("共\d+页", r.text)
                # if m:
                #     self.max_page = m.group()
                self.max_page = int(dom.cssselect(".pages > ul > li > a")[0].text.replace('共', '').replace('页: ', ''))

            img_dom = dom.cssselect('p[align="center"] > a > img')
            if len(img_dom) == 0:
                img_dom = dom.cssselect('p[align="center"] > img')
            if len(img_dom) == 0:
                img_dom = dom.cssselect('#big-pic > a > img')
            if len(img_dom) == 0:
                img_dom = dom.cssselect('center > a > img')
            if len(img_dom) == 0:
                raise Exception('出现新的解析规则')

            photo_title = img_dom[0].get('alt') + "_" + str(page)
            photo_url = img_dom[0].get('src')

            return {'photo_title': photo_title, 'photo_url': photo_url}
        except:
            print("失败的url:", url)
            print(traceback.print_exc())

    def main(self):
        count = self.min_page
        while count <= self.max_page:
            tu = self.detail_pagnation(count)
            if tu is not None:
                yield tu
            count += 1


if __name__ == '__main__':
    ca = Categories()

    for tag in ca.main():
        print("0", tag)
        xp = XiangcePagnation(tag)

        for xiangce_hrefs in xp.main():
            print("1", xiangce_hrefs)
            for xh in xiangce_hrefs:
                dp = DetailPagnation(xh)
                for photo_title_url in dp.main():
                    print("2", photo_title_url)

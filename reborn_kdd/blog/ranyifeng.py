import requests
from lxml import etree


def get_categories():
    url = "http://www.ruanyifeng.com/blog/archives.html"
    r = requests.get(url, verify=False, headers={"connection": "close"})
    page = etree.HTML(r.text)
    css = "#beta-inner > div > div > ul > li  > a"
    cats = page.cssselect(css)
    urls = []
    for cat in cats:
        href = cat.get('href')
        urls.append(href)

    return urls


def pag_article_list(url):
    r = requests.get(url,
                     verify=False,
                     headers={
                         "connection": "close",
                         # "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36"
                     })
    r.encoding = 'utf-8'
    page = etree.HTML(r.text)

    css = "#alpha-inner > div > div > ul > li> a"
    als = page.cssselect(css)

    title_urls = []
    for al in als:
        title_urls.append({
            'title': al.text,
            'url': al.get("href")
        })

    return title_urls


if __name__ == '__main__':
    for url in get_categories():
        print(pag_article_list(url))

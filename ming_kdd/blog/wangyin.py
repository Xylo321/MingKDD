import requests
from lxml import etree


def pag_article_list():
    url = "http://www.yinwang.org"
    r = requests.get(url, verify=False, headers={
        'connection': 'close',
        'user-agent': 'I am a crawler programmer and my crawler likes your blog.'
    })
    page = etree.HTML(r.text)

    als = page.cssselect(".list-group-item > a")

    article_list = []
    for al in als:
        href = al.get('href')
        title = al.text
        article_list.append({
            'url': url + href,
            'title': title
        })

    return article_list


if __name__ == "__main__":
    print(pag_article_list())
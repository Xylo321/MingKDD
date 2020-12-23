"""
http://www.9610.com/map.htm
"""
from lxml import etree
import requests
import logging


def get_categories():
    url = 'http://www.9610.com/map.htm'
    css = 'tr:nth-child(3)'
    r = requests.get(url)
    r.raise_for_status()
    r.encoding = 'gb2312'
    parser = etree.HTML(r.text)
    tr_dom = parser.cssselect(css)
    if len(tr_dom) == 0:
        raise
    a_doms = tr_dom[0].cssselect("a")
    cats = [{
        'name': a_dom.text,
        'url': 'http://www.9610.com/' + a_dom.get('href')
    } for a_dom in a_doms]
    logging.debug('cats: %s', str(cats))
    return cats


def get_photos(cat_url):
    photos = []
    if '100.htm' in cat_url:
        photos = _100(cat_url)
    logging.debug('photos: %s', str(photos))
    return photos


def _100(cat_url):
    photos = []
    r = requests.get(cat_url)
    r.raise_for_status()
    r.encoding = 'gb2312'

    css = 'li > a'
    parser = etree.HTML(r.text)
    a_doms = parser.cssselect(css)
    photos = [
        {
            'name': a_dom.text,
            'url': 'http://www.9610.com/' + a_dom.get('href')
        } for a_dom in a_doms
    ]
    return photos
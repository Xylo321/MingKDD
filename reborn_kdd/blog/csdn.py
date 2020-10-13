import time
import traceback

from reborn_kdd.google_driver.chrome_driver import ChromeDriver


class CSDN(object):
    def get_categories(self):
        cats = []
        driver = None
        try:
            driver = ChromeDriver(headless=True)
            url = "https://blog.csdn.net/nav/web"
            driver.open(url, 5)

            lias = driver.cssselect(".nav_com > ul > li > a")
            for lia in lias:
                href = lia.get_attribute('href')
                text = lia.text
                if text not in ['动态', '推荐', '人工智能', '区块链']:
                    cats.append(href)
        except:
            print(traceback.format_exc())
        finally:
            if driver: driver.close()

        return cats

    def page_article_list(self, url, times):
        driver = None
        try:
            driver = ChromeDriver(headless=True)
            driver.open(url, 30)
            ti = 0
            while ti < times:
                print("%s times:" % url, ti)
                wes = driver.cssselect("#feedlist_id > li > div > div.title > h2 > a")

                title_urls = []
                for a in wes:
                    title_urls.append({
                        'title': a.text,
                        'url': a.get_attribute("href")
                    })

                if len(title_urls) == 0:
                    driver.xila_gundongtiao(0)
                    time.sleep(2)
                    driver.xila_gundongtiao(10000)

                    time.sleep(5)
                    continue
                else:
                    yield title_urls

                    driver.del_ziyuansu("#feedlist_id", "#feedlist_id > li")
                    driver.xila_gundongtiao(0)
                    time.sleep(2)
                    driver.xila_gundongtiao(10000)

                ti += 1
        except:
            print(traceback.format_exc())
        finally:
            if driver: driver.close()

    def get_article_detail(self, url):
        pass


if __name__ == '__main__':
    c = CSDN()
    cats = c.get_categories()
    print(cats)
    for tus in c.page_article_list(cats[2], 1000):
        print(tus)
    # c.get_article_detail("https://blog.csdn.net/csdnnews/article/details/103097703", proxies={"http": "http://localhost:8888"})

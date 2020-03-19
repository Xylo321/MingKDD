import os
import platform
import time
import traceback

from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options


class ChromeDriver(object):
    def __init__(self, headless=False):
        executable_path = ''
        pl = platform.platform()
        if 'macOS' in pl:
            executable_path = "%s%schromedriver_mac" % (os.path.split(os.path.abspath(__file__))[0], os.path.sep)
        elif 'Linux' in pl:
            executable_path = "%s%schromedriver_linux" % (os.path.split(os.path.abspath(__file__))[0], os.path.sep)
        else:
            raise Exception('Unknown Operation System Type')

        options = Options()
        # 不会显示您的浏览器正在自动化信息
        options.add_experimental_option('excludeSwitches', ['enable-automation'])

        # 是否显示界面
        if headless == True:
            # options.add_argument('headless')
            options.headless = headless
        self.driver = Chrome(executable_path=executable_path, options=options)

    def open(self, url, page_load_time_out=5):
        self.driver.get(url)
        self.driver.set_page_load_timeout(page_load_time_out)

    def cssselect(self, css_selector):
        return self.driver.find_elements_by_css_selector(css_selector)

    def get_page_source(self):
        return self.driver.page_source

    def wait_element_load(self, css_selector, loop_time, timeout):
        for i in range(loop_time):
            wes = self.cssselect(css_selector)
            if len(wes) == 0:
                # self.driver.implicitly_wait(timeout)
                time.sleep(timeout)
            else:
                return wes

        # raise Exception("wait element load timeout loop_time failed")

    def del_ziyuansu(self, parent_css, child_css):
        parent_dom = self.driver.find_element_by_css_selector(parent_css)
        child_doms = self.cssselect(child_css)
        for child_dom in child_doms:
            self.driver.execute_script("arguments[0].removeChild(arguments[1])", parent_dom, child_dom)

    def xila_gundongtiao(self, scroll_num, wait_time=5):
        js = "var q=document.documentElement.scrollTop=%d" % scroll_num
        self.driver.execute_script(js)
        time.sleep(wait_time)

    def close(self):
        if self.driver:
            self.driver.quit()


if __name__ == '__main__':
    cd = ChromeDriver(headless=False)
    try:
        cd.open("https://cn.mouser.com")
    except:
        print(traceback.format_exc())
    finally:
        cd.close()

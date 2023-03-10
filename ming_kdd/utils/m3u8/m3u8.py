import os
import re
import queue
import requests
import shutil
import platform
import shlex
import subprocess
import logging

requests.packages.urllib3.disable_warnings()
from concurrent.futures import ThreadPoolExecutor


class _ThreadPoolExecutorWithQueueSizeLimit(ThreadPoolExecutor):
    """
    实现多线程有界队列
    队列数为线程数的2倍
    """

    def __init__(self, max_workers=None, *args, **kwargs):
        super().__init__(max_workers, *args, **kwargs)
        self._work_queue = queue.Queue(max_workers * 2)


def make_sum():
    ts_num = 0
    while True:
        yield ts_num
        ts_num += 1


class M3u8Download:
    """
    :param url: 完整的m3u8文件链接 如"https://www.bilibili.com/example/index.m3u8"
    :param name: 保存m3u8的文件名 如"index"
    :param max_workers: 多线程最大线程数
    :param num_retries: 重试次数
    """

    def __init__(self, url, name, max_workers=64, num_retries=5):
        self.url = url
        self.name = name
        self.max_workers = max_workers
        self.num_retries = num_retries
        self.front_url = None
        self.ts_url_list = []
        self.success_sum = 0
        self.ts_sum = 0
        self.log = logging.getLogger('M3u8Download')

        self.download_success = False
        self.headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) \
        AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.105 Safari/537.36'}

        try:
            self.get_m3u8_info(self.url, self.num_retries)
            with _ThreadPoolExecutorWithQueueSizeLimit(self.max_workers) as pool:
                for ts_url, auto_id in zip(self.ts_url_list, range(0, len(self.ts_url_list))):
                    pool.submit(self.download_ts, ts_url, auto_id, self.num_retries)

            if self.success_sum == self.ts_sum:
                self.log.info('下载成功: %s', self.name)
                self.output_mp4()
                self.download_success = True
            else:
                raise Exception('下载失败: %s' % self.name)
        except Exception as e:
            self.log.error(str(e))
        finally:
            # os.system(f'rm -rf ./{self.name} ./{self.name}.m3u8')
            cwp = os.getcwd() + os.path.sep
            task_dir = cwp + self.name
            m3u8_file = cwp + self.name + '.m3u8'
            if os.path.exists(task_dir):
                shutil.rmtree(task_dir)
            if os.path.exists(m3u8_file):
                os.remove(m3u8_file)

    def get_m3u8_info(self, m3u8_url, num_retries):
        """
        获取m3u8信息
        """
        try:
            res = requests.get(m3u8_url, timeout=(10, 30), verify=False, headers=self.headers)
            if res.status_code == 404:
                return
            self.front_url = res.url.split(res.request.path_url)[0]
            if "EXT-X-STREAM-INF" in res.text:  # 判定为顶级M3U8文件
                for line in res.text.split('\n'):
                    if "#" in line:
                        continue
                    elif re.search(r'^http', line) is not None:
                        self.url = line
                    elif re.search(r'^/', line) is not None:
                        self.url = self.front_url + line
                    else:
                        self.url = self.url.rsplit("/", 1)[0] + '/' + line
                self.get_m3u8_info(self.url, self.num_retries)
            else:
                m3u8_text_str = res.text
                self.get_ts_url(m3u8_text_str)
        except Exception as e:
            if num_retries > 0:
                self.get_m3u8_info(m3u8_url, num_retries - 1)

    def get_ts_url(self, m3u8_text_str):
        """
        获取每一个ts文件的链接
        """
        if not os.path.exists(f"./{self.name}"):
            os.mkdir(f"./{self.name}")
        new_m3u8_str = ''
        ts = make_sum()
        for line in m3u8_text_str.split('\n'):
            if "#" in line:
                if "EXT-X-KEY" in line and "URI=" in line:
                    key = self.download_key(line, 5)
                    if key:
                        new_m3u8_str += f'{key}\n'
                        continue
                new_m3u8_str += f'{line}\n'
                if "EXT-X-ENDLIST" in line:
                    break
            elif re.search(r'^http', line) is not None:
                new_m3u8_str += f"./{self.name}/{next(ts)}\n"
                self.ts_url_list.append(line)
            elif re.search(r'^/', line) is not None:
                new_m3u8_str += f"./{self.name}/{next(ts)}\n"
                self.ts_url_list.append(self.front_url + line)
            else:
                new_m3u8_str += f"./{self.name}/{next(ts)}\n"
                self.ts_url_list.append(self.url.rsplit("/", 1)[0] + '/' + line)
        self.ts_sum = next(ts)
        with open(f"./{self.name}.m3u8", "w", encoding='utf-8') as f:
            f.write(new_m3u8_str)

    def download_ts(self, ts_url, save_ts_name, num_retries):
        """
        下载 .ts 文件
        """
        ts_url = ts_url.split('\n')[0]
        try:
            if not os.path.exists(f"./{self.name}/{save_ts_name}"):
                res = requests.get(ts_url, stream=True, timeout=(5, 30), verify=False, headers=self.headers)
                if res.status_code == 404:
                    return
                if res.status_code == 200:
                    with open(f"./{self.name}/{save_ts_name}", "wb") as ts:
                        for chunk in res.iter_content(chunk_size=1024):
                            if chunk:
                                ts.write(chunk)
                    self.success_sum += 1
                    self.log.info(f"Downloading {self.name}：{self.success_sum}/{self.ts_sum}")
                else:
                    self.download_ts(ts_url, save_ts_name, num_retries - 1)
                res.close()
            else:
                self.success_sum += 1
        except Exception:
            if os.path.exists(f"./{self.name}/{save_ts_name}"):
                os.remove(f"./{self.name}/{save_ts_name}")
            if num_retries > 0:
                self.download_ts(ts_url, save_ts_name, num_retries - 1)

    def download_key(self, key_line, num_retries):
        """
        下载key文件
        """
        mid_part = re.search(r"URI=[\'|\"].*?[\'|\"]", key_line).group()
        may_key_url = mid_part[5:-1]
        if re.search(r'^http', may_key_url) is not None:
            true_key_url = may_key_url
        elif re.search(r'^/', may_key_url) is not None:
            true_key_url = self.front_url + may_key_url
        else:
            true_key_url = self.url.rsplit("/", 1)[0] + '/' + may_key_url
        try:
            res = requests.get(true_key_url, timeout=(10, 30), verify=False, headers=self.headers)
            if res.status_code == 404:
                return
            with open(f"./{self.name}/key", 'wb') as f:
                f.write(res.content)
            res.close()
            return f'{key_line.split(mid_part)[0]}URI="./{self.name}/key"{key_line.split(mid_part)[-1]}'
        except Exception as e:
            if os.path.exists(f"./{self.name}/key"):
                os.remove(f"./{self.name}/key")
            self.log.info("加密视频,无法加载key,揭秘失败")
            if num_retries > 0:
                self.download_key(key_line, num_retries - 1)

    def output_mp4(self):
        """
        合并.ts文件，输出mp4格式视频，需要ffmpeg
        """
        cmd_path = os.path.split(os.path.abspath(__file__))[0] + os.path.sep + 'ffmpeg'

        if platform.platform().startswith('macOS') or platform.platform().startswith('Linux'):
            cmd_path = 'ffmpeg'

        cmd_tpl = '%s -allowed_extensions ALL -i %s.m3u8 -acodec copy -vcodec copy -f mp4 "%s.mp4"'
        # if platform.platform().startswith('macOS') or platform.platform().startswith('Linux'):
        #     cmd_tpl = '%s -allowed_extensions ALL -i %s.m3u8 -acodec copy -vcodec copy -f mp4 "%s.mp4"'

        cmd = cmd_tpl % (
            cmd_path, self.name, self.name
        )
        if platform.platform().startswith('macOS') or platform.platform().startswith('Linux'):
            cmd = cmd.replace('(', '\(').replace(')', '\)')
            args = shlex.split(cmd)
            p = subprocess.Popen(args)
            if p.wait() != 0:
                raise Exception('合并ts文件，输出mp4视频失败: %s' % self.name)
        else:
            os.system(cmd)


def download_m3u8_video(url_list, name_list, mw=30, nr=5):
    # 如果M3U8_URL的数量 ≠ SAVE_NAME的数量
    # 下载一部电视剧时，只需要输入一个name就可以了
    start_num = 1
    md = None
    for i in range(len(url_list)):
        md = M3u8Download(url_list[i],
                         name_list[i] if len(url_list) == len(name_list) else f"{name_list[0]}{start_num:02}",
                         max_workers=mw,
                         num_retries=nr
                         )

        start_num += 1

    return md.download_success


if __name__ == '__main__':
    url_list = input("输入url，若同时输入多个url时要用空格分开：").split()
    name_list = input("输入name，若同时输入多个name要用空格分开：").split()
    download_m3u8_video(url_list, name_list)

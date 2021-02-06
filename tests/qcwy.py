import requests
import json
import time
import random
import csv
from lxml import etree


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
}

def search(keyword: str, page: int) -> dict:
    global HEADERS
    url: str = 'https://search.51job.com/list/030200,000000,0000,00,9,99,%s,2,%d.html?lang=c&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&ord_field=0&dibiaoid=0&line=&welfare='
    r: requests.Response = requests.get(url % (keyword, page), headers=HEADERS)

    r.raise_for_status()
    lines: list = r.text.split('\n')
    i: int = len(lines) - 1
    data_line: str = None
    while i > 0:
        if lines[i].startswith('window.__SEARCH_RESULT__ = '):
            data_line = lines[i].replace('window.__SEARCH_RESULT__ = ', '').replace('</script>', '')
        i -= 1
    if data_line is not None:
        return json.loads(data_line)
    raise


def delivery(job_ids: list) -> None:
    global HEADERS
    jobid = str(tuple(job_id + ":0" for job_id in job_ids)).replace('"', '').replace("'", '').replace(' ', '')
    url_params: dict = {
        "rand": str(random.random()),
        "jsoncallback": "jQuery%d_%d" % (int(time.time()), int(time.time())),
        "jobid": jobid,
        "prd": "search.51job.com",
        "prp": "01",
        "cd": "search.51job.com",
        "cp": "01",
        "elementname": "delivery_jobid",
        "deliverytype": "2",
        "deliverydomain": "//i.51job.com",
        "language": "c",
        "imgpath": "//img01.51jobcdn.com",
        "_": int(time.time())
    }
    url: str = 'https://i.51job.com/delivery/delivery.php'
    r: requests.Response = requests.get(url, params=url_params, headers={
        'User-Agent': HEADERS['User-Agent'],
        'Cookie': 'guid=a7a651ac1e775a046aa32007639c5833; nsearch=jobarea%3D%26%7C%26ord_field%3D%26%7C%26recentSearch0%3D%26%7C%26recentSearch1%3D%26%7C%26recentSearch2%3D%26%7C%26recentSearch3%3D%26%7C%26recentSearch4%3D%26%7C%26collapse_expansion%3D; ps=needv%3D0; 51job=cuid%3D109138045%26%7C%26cusername%3Dphone_15728567842%26%7C%26cpassword%3D%26%7C%26cname%3D%25CA%25A9%25B4%25CF%26%7C%26cemail%3Dcongshi.hello%2540gmail.com%26%7C%26cemailstatus%3D3%26%7C%26cnickname%3D%25D3%25C7%25D3%25F4%25B5%25C4%25C3%25AB%25C3%25AB%26%7C%26ccry%3D.0wUMWp2Zl.as%26%7C%26cconfirmkey%3DcoJKLd9kSd8TY%26%7C%26cautologin%3D1%26%7C%26cenglish%3D0%26%7C%26sex%3D0%26%7C%26cnamekey%3DcoODTSZyRiZ1I%26%7C%26to%3D835c5e2ca6342a98f0fb11d521406db15fa2378c%26%7C%26; m_search=areacode%3D040000; slife=lastlogindate%3D20210121%26%7C%26; search=jobarea%7E%60010000%7C%21ord_field%7E%600%7C%21recentSearch0%7E%60010000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FAaf%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch1%7E%60010000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FApython%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch2%7E%60030200%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch3%7E%60010000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FApy%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21recentSearch4%7E%60010000%A1%FB%A1%FA000000%A1%FB%A1%FA0000%A1%FB%A1%FA00%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA99%A1%FB%A1%FA9%A1%FB%A1%FA99%A1%FB%A1%FA%A1%FB%A1%FA0%A1%FB%A1%FA%B1%B1%BE%A9%B5%E3%BB%F7%BF%C6%BC%BC%A1%FB%A1%FA2%A1%FB%A1%FA1%7C%21',
        'Host': 'i.51job.com'
    })
    r.raise_for_status()
    print(r.text)


def job_request(href: str) -> list:
    r: requests.Response = requests.get(href, headers=HEADERS)
    r.raise_for_status()
    r.encoding = 'gbk'
    page = etree.HTML(r.text)
    ps: list = page.cssselect('.job_msg > p')
    results = []
    for p in ps:
        try:
            if p.text.strip() == '&nbsp;':
                continue
            results.append(p.text.split('.', 1)[1].strip())
        except:
            pass
    return results


def auto_delivery() -> None:
    current_page: int = 1
    total_pages: int = None
    keyword: str = input('输入职位的关键字：')
    f = open('投递报告(%s).csv' % keyword, 'a')
    csv_w = csv.writer(f)
    while 1:
        data: dict = search(keyword, current_page)
        # pprint.pprint(data)
        esr: list = data['engine_search_result']

        if current_page == 1:
            total_pages: int = int(data['total_page'])
            csv_w.writerow(esr[0].keys())
        csv_w.writerows([element.values() for element in esr])

        # pprint.pprint(esr)
        job_ids: list = [element['jobid'] for element in esr]

        delivery(job_ids)

        current_page += 1
        if current_page > total_pages:
            print('投递结束')
            break

    f.close()


def stat_job_request():
    current_page = 1
    total_pages = None
    keyword = input('输入职位的关键字：')

    stat = dict()
    while 1:
        data: dict = search(keyword, current_page)
        # pprint.pprint(data)
        esr: list = data['engine_search_result']
        job_hrefs = [element['job_href'] for element in esr]

        for job_href in job_hrefs:
            try:
                job_reqs: list= job_request(job_href)
                print(job_reqs)
                for jr in job_reqs:
                    if stat.get(jr, None) is None:
                        stat[jr] = 1
                    else:
                        stat[jr] += 1
            except:
                pass

        if current_page == 1:
            total_pages: int = int(data['total_page'])

        current_page += 1
        if current_page > total_pages:
            print('统计结束')
            break

    f = open('职位要求统计报告(%s).csv' % (keyword + str(int(time.time()))), 'w')
    csv_w = csv.writer(f)
    for k, v in stat.items():
        csv_w.writerow([k, v])

    f.close()


if __name__ == '__main__':
    print('前程无忧爬虫。')
    while 1:
        choice = input('输入"1"选择投递简历，输入"2"统计职位要求，输入"3"退出：')
        if choice == "1":
            auto_delivery()
        elif choice == "2":
            stat_job_request()
        elif choice == "3":
            break
        else:
            print('输入错误')
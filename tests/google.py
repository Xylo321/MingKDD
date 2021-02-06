import requests
proxies={
    'http': 'http://127.0.0.1:1080',
    'https': 'https://127.0.0.1:1080'
}
r = requests.get('https://www.google.com.hk', proxies=proxies, verify=False, timeout=10)
print(r.text)
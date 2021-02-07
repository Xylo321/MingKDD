import requests
proxies={
    'http': 'socks5://localhost:1080',
    'https': 'socks5://localhost:1080'
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
}
r = requests.get('https://www.youtube.com', proxies=proxies, verify=False, timeout=10, headers=headers)
print(r.text)
import requests


def download_file(url, save_file_name):
    r = requests.get(url)
    with open(save_file_name, "wb") as code:
      code.write(r.content)
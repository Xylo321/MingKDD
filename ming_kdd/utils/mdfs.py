import requests
from requests_toolbelt import MultipartEncoder
from mimetypes import guess_type


def upload(upload_url, api_key, third_user_id, category_id, title, file_name, file_path):
    m = MultipartEncoder(fields={
        'api_key': api_key,
        'category_id': str(category_id),
        'third_user_id': str(third_user_id),
        'title': title,
        'upload_file_name': (file_name, open(file_path, 'rb'), guess_type(file_name)[0] or "application/octet-stream")
    })
    r = requests.post(upload_url, data=m, headers={'Content-Type': m.content_type}, verify=False)
    r.raise_for_status()
    jd = r.json()
    if jd['status'] == 0:
        return 0
    elif jd['status'] == 1:
        return 1
    else:
        raise
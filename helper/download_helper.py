import os
import urllib.request
import requests
from helper.multithread_helper import multithread_helper


def download(url, file_name, timeout=60):
    with open(file_name, "wb") as file:
        response = requests.get(url, timeout=timeout)
        file.write(response.content)


def download_from_url(url, file_output_path):
    try:
        # print('downloading ', url, 'in', file_output_path)
        os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
        urllib.request.urlretrieve(url, file_output_path)
    except Exception as e:
        print(e)


def download_from_url_obj(url_obj):
    # print('prepare download: ', url_obj)
    url = url_obj['url']
    file_output_path = url_obj['file_output_path']
    print(f'downloading {url}')
    os.makedirs(os.path.dirname(file_output_path), exist_ok=True)
    urllib.request.urlretrieve(url, file_output_path)
    return ''


def download_from_batch(urls, folder_output_path, max_workers=10, timeout_concurrent_by_second=600 * 60):
    print('prepare download from batch')
    pos = 0
    urls_obj = []
    for link in urls:
        pos += 1
        # if (pos > 5):
        #     continue
        file_name = link.split('/')[-1]
        # print(link)
        url_obj = {'url': link, 'file_output_path': folder_output_path + '/' + file_name}
        # print('url_obj: ', url_obj )
        urls_obj.append(url_obj)
        # download_from_url(link, folder_output_path + '/' + file_name)
    # print(urls_obj)
    multithread_helper(items=urls_obj, method=download_from_url_obj, max_workers=max_workers, debug=False,
                       timeout_concurrent_by_second=timeout_concurrent_by_second)

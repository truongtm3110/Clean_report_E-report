import re
import urllib
from typing import Union
from urllib.parse import urlparse, parse_qs, unquote, urlencode
from requests.utils import to_key_val_list
import requests


def extract_domain(url):
    return urlparse(url).netloc


def extract_domain_without_www(url):
    return urlparse(url).netloc.replace('www.', '')


def get_root_domain(url):
    from tld import get_tld
    res = get_tld(url, as_object=True)
    return res.fld


def normalize_url(url):
    if url.startswith('//'):
        url = 'https://' + url[2:]
    if url[-1] == '/':
        return url[:-1]
    return url


def get_last_folder_from_url(url):
    url_normalize = normalize_url(url)
    return url_normalize.split('/')[-1]


# print(extract_domain('https://www.shopee.vn/shop/68617354/search?order=asc'))

def get_params_from_url(url):
    return parse_qs(urlparse(url).query)


def get_url_remove_all_params(url, param_keep='spid'):
    # if len (url.split('?')) > 1 and
    params = get_params_from_url(url)
    if param_keep and len(url.split('?')) > 1 and extract_domain_without_www(url).startswith('tiki.vn') and params.get(
            'spid') is not None:
        return url.split('?')[0] + '?' + param_keep + '=' + str(params.get('spid')[0])
    elif param_keep and len(url.split('?')) > 1 and extract_domain_without_www(url).startswith(
            'adayroi.com') and params.get(
        'offer') is not None:
        return url.split('?')[0] + '?offer' + '=' + str(params.get('offer')[0])

    return url.split('?')[0]


def get_url_without_params(url):
    if url:
        url = url.split('?', 1)[0]
        url = url.split('&', 1)[0]
    return url


def decode_url_string(url_string):
    return unquote(url_string)


def encode_url_string(url_decoded):
    # return urllib.parse.urlencode(url_decoded)
    return urllib.parse.quote(url_decoded)


def unshorten_url(url):
    response = requests.get(url, allow_redirects=False)
    meta_location = response.headers.get("Location")
    if not meta_location:
        match = re.search(r"window.location.href\s=\s\"(.+)\"", response.text)
        if match:
            return match.group(1)
        return url
    return get_redirect_url(meta_location)


def add_params(url, params):
    from furl import furl
    return furl(url).add(params).url


def get_redirect_url(url):
    try:
        redirect_url = re.search("(?<=url=).+", url).group()
    except AttributeError:
        redirect_url = unshorten_url(url)
    else:
        redirect_url = urllib.parse.unquote(redirect_url)
    return get_url_without_params(redirect_url)


def remove_http_https_protocol(url):
    normalized_url = normalize_url(url)
    return re.sub("(http|https)://", "", normalized_url)

def encode_params(data) -> Union[str, bytes]:
    """Encode parameters in a piece of data.

    Will successfully encode parameters when passed as a dict or a list of
    2-tuples. Order is retained if data is a list of 2-tuples but arbitrary
    if parameters are supplied as a dict.
    """

    if isinstance(data, (str, bytes)):
        return data
    elif hasattr(data, 'read'):
        return data
    elif hasattr(data, '__iter__'):
        result = []
        for k, vs in to_key_val_list(data):
            if isinstance(vs, (str, bytes)) or not hasattr(vs, '__iter__'):
                vs = [vs]
            for v in vs:
                if v is not None:
                    result.append(
                        (k.encode('utf-8') if isinstance(k, str) else k,
                         v.encode('utf-8') if isinstance(v, str) else v))
        return urlencode(result, doseq=True)
    else:
        return data

# url = 'https://tiki.vn/smart-tivi-lg-43-inch-4k-uhd-43uk6340ptf-p2172117.html?spid=2237437&src=lp-296'
# print(get_url_remove_all_params(url))
if __name__ == '__main__':
    # url = "https://www.google.com/search?q=sạc dự phòng xiaomi site:tiki.vn OR site:lazada.vn  OR site:shopee.vn OR site:sendo.vn&tbm=isch&biw=1920&bih=976"
    # from furl import furl
    #
    # f = furl(url)
    # print(f.url)
    # match = re.search("/c(\d+)\?src=mega-menu$", "https://tiki.vn/dien-thoai-may-tinh-bang/c1789?src=mega-menu")
    # print(match)
    # result = normalize_url(get_url_without_params("https://www.lazada.vn/cham-soc-da-mat/?ajax=true"))
    # print(result)
    # url = "https://s.taobao.com/search?q=%E7%94%B7%E8%A3%85%2F%E7%94%B7%E5%A3%AB%E7%B2%BE%E5%93%81&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_20210402&ie=utf8"
    print(get_params_from_url(urllib.parse.unquote_plus(
        "ios-app://959841449/shopeevn/home?navRoute=eyJwYXRocyI6IFt7IndlYk5hdiI6IHsidXJsIjogImh0dHBzOi8vc2hvcGVlLnZuL3Nob3AvP3NpZD02MTE4MDY1MzUmc3U9bGVpY2F2aWV0bmFtJl9nc3JjPWh0dHBzJTNBLy9zaG9wZWUudm4vc2hvcC8lM0ZzaWQlM0Q2MTE4MDY1MzUlMjZzdSUzRGxlaWNhdmlldG5hbSJ9fV19",
        "utf-8")).get("navRoute", [None])[0])
    import base64

    print(base64.b64decode(
        "eyJwYXRocyI6IFt7IndlYk5hdiI6IHsidXJsIjogImh0dHBzOi8vc2hvcGVlLnZuL3Nob3AvP3NpZD02MTE4MDY1MzUmc3U9bGVpY2F2aWV0bmFtJl9nc3JjPWh0dHBzJTNBLy9zaG9wZWUudm4vc2hvcC8lM0ZzaWQlM0Q2MTE4MDY1MzUlMjZzdSUzRGxlaWNhdmlldG5hbSJ9fV19"))
    # url = unshorten_url("https://s.lazada.vn/s.1P2mA")
    # print(url)

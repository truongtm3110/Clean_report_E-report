import datetime
import gzip
import io
import urllib

import requests
from bs4 import BeautifulSoup
from lxml import etree

from helper.error_helper import log_error
from helper.reader_helper import get_content_by_gz

SITEMAP_DATETIME_FORMATS = ['%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%d']


def extract_sitemap_gz(file_sitemap_gz_path):
    # content_xml = get_content_by_gz('/bee/data/shopee.vn/crawler/sitemap/20190521_144551_229/sitemap.items-3.xml.gz')
    content_xml = get_content_by_gz(file_sitemap_gz_path)
    # soup = BeautifulSoup(content_xml, 'lxml')
    # soup = BeautifulSoup(content_xml, features="html.parser")
    soup = BeautifulSoup(content_xml, features="html.parser")
    # sitemapTags = soup.find_all("sitemap")
    output_url_and_lastmod = []
    for item in soup.find_all('url'):
        url = item.loc.contents[1] if item.loc.contents is not None and len(item.loc.contents) > 1 and len(
            item.loc.contents[1]) > 1 else \
            item.loc.contents[0]
        lastmod = item.lastmod.get_text()
        url_and_lastmod = {'url': url, 'lastmod': lastmod}
        if item.find('image:image') is not None:
            url_image = item.find('image:image').find('image:loc').contents[0]
            caption = item.find('image:image').find('image:caption').contents[0]
            url_and_lastmod.update({'url_image': url_image, 'caption': caption})

        output_url_and_lastmod.append(url_and_lastmod)
    return output_url_and_lastmod


def extract_sitemap_gz_v2(file_sitemap_gz_path):
    import xml.dom.minidom
    def get_all_text(node):
        if node.nodeType == node.TEXT_NODE or node.nodeType == 4:
            return node.data.strip().strip('\n')
        else:
            text_string = ""
            for child_node in node.childNodes:
                text_string += get_all_text(child_node)
            return text_string

    output_url_and_lastmod = []
    try:
        with gzip.open(file_sitemap_gz_path, 'rt') as f:
            content = f.read()
            doc = xml.dom.minidom.parseString(content)
            url_tags = doc.getElementsByTagName('url')
            for url_tag in url_tags:
                loc = get_all_text(url_tag.getElementsByTagName('loc')[0])
                lastmod = get_all_text(url_tag.getElementsByTagName('lastmod')[0])
                output_url_and_lastmod.append({'url': loc, 'lastmod': lastmod})
        return output_url_and_lastmod
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(file_sitemap_gz_path)
        return None


def get_gzip_text_from_url(url):
    file_content = None
    try:
        web_response = requests.get(url).content
        f = io.BytesIO(web_response)
        # with urllib.request.urlopen(url) as response:
        # response = requests.get(url).text
        with gzip.GzipFile(fileobj=f) as uncompressed:
            file_content = uncompressed.read()
            # print(f'file_content: {file_content}')
    except Exception as e:
        print(e)
    finally:
        return file_content


def get_text_from_url(url):
    try:
        return requests.get(url, timeout=10).text
    except Exception as e:
        print(e)
        return None


def convert_lastmod_to_second(lastmod_string):
    try:
        date_time_obj = datetime.datetime.strptime(lastmod_string, '%Y-%m-%dT%H:%M:%S%z')
        return int(datetime.datetime.timestamp(date_time_obj))
    except Exception as e:
        return None


def extract_log_url_from_sitemap(content_string, time__func_formatter=convert_lastmod_to_second):
    # soup = BeautifulSoup(content_string, features="html.parser")
    soup = BeautifulSoup(content_string, features="xml")
    url_obj_list = []
    # for item in soup.find_all('url'):
    for item in soup.select('url, sitemap'):
        url = item.loc.contents[1] if item.loc.contents is not None and len(item.loc.contents) > 1 and len(
            item.loc.contents[1]) > 1 else \
            item.loc.contents[0]
        lastmod = item.lastmod.get_text() if item.lastmod is not None else None
        url_obj_list.append({'url': url, 'lastmod': time__func_formatter(lastmod)})
    return url_obj_list


def extract_log_url_from_sitemap_by_lxml(content_string, time__func_formatter=convert_lastmod_to_second):
    url_obj_list = []
    doc = etree.fromstring(content_string)
    all_url = doc.findall("url", namespaces=doc.nsmap)
    for url in all_url:
        loc = url.find("loc", namespaces=url.nsmap).text.strip()
        lastmod = url.find("lastmod", namespaces=url.nsmap).text
        url_obj_list.append({
            "url": loc,
            "lastmod": time__func_formatter(lastmod)
        })
    return url_obj_list


def extract_xlink_from_sitemap_by_lxml(content_string, time__func_formatter=convert_lastmod_to_second):
    xlink_list = []
    doc = etree.fromstring(content_string)
    etree.register_namespace("xhtml", "http://www.w3.org/1999/xhtml")
    all_url = doc.findall("url", namespaces=doc.nsmap)
    for url in all_url:
        loc = url.find("{http://www.w3.org/1999/xhtml}link").get("href").strip()
        lastmod = url.find("lastmod", namespaces=url.nsmap).text
        xlink_list.append({
            "url": loc,
            "lastmod": time__func_formatter(lastmod)
        })
    return xlink_list


def get_all_loc_from_url_gzip(sitemap_url, max_day_diff=None):
    content_sitemap = get_gzip_text_from_url(sitemap_url)
    return get_all_loc_by_lxml(content_sitemap, max_day_diff)


def get_all_loc_from_url_text(sitemap_url):
    content_sitemap = get_text_from_url(sitemap_url)
    return get_all_loc_by_lxml(content_sitemap)


def get_all_loc(content_sitemap, max_day_diff=None):
    soup = BeautifulSoup(content_sitemap, features='html.parser')
    # soup = BeautifulSoup(content_sitemap, features='xml')
    urls = soup.find_all('url')
    all_loc = []
    for url in urls:
        loc = url.select_one('loc').get_text()
        if max_day_diff is not None:
            day_diff = None
            for sitemap_datetime_format in SITEMAP_DATETIME_FORMATS:
                try:
                    last_mod = datetime.datetime.strptime(url.select_one('lastmod').get_text(), sitemap_datetime_format)
                    day_diff = (datetime.datetime.now(datetime.timezone.utc) - last_mod.astimezone(
                        datetime.timezone.utc)).days
                    break
                except ValueError:
                    continue
            if day_diff is None:
                print("date time in sitemap is not valid")
            elif day_diff < max_day_diff:
                all_loc.append(loc)
        else:
            all_loc.append(loc)
    return all_loc


def get_all_loc_by_lxml(content_sitemap, max_day_diff=None):
    all_loc = []
    try:
        doc = etree.fromstring(content_sitemap)
        all_url = doc.findall("url", namespaces=doc.nsmap)
        for url in all_url:
            loc = url.find("loc", namespaces=url.nsmap).text.strip()
            if max_day_diff is not None:
                day_diff = None
                for sitemap_datetime_format in SITEMAP_DATETIME_FORMATS:
                    try:
                        last_mod = datetime.datetime.strptime(url.find("lastmod", namespaces=url.nsmap).text,
                                                              sitemap_datetime_format)
                        day_diff = (datetime.datetime.now(datetime.timezone.utc) - last_mod.astimezone(
                            datetime.timezone.utc)).days
                        break
                    except ValueError:
                        pass
                if day_diff is None:
                    print("date time in sitemap is not valid")
                elif day_diff < max_day_diff:
                    all_loc.append(loc)
            else:
                all_loc.append(loc)
    except Exception as e:
        log_error(e)
    return all_loc


def get_all_sitemap_loc_by_lxml(content_sitemap, max_day_diff=None):
    all_loc = []
    doc = etree.fromstring(content_sitemap)
    all_url = doc.findall("sitemap", namespaces=doc.nsmap)
    for url in all_url:
        loc = url.find("loc", namespaces=url.nsmap).text.strip()
        if max_day_diff is not None:
            day_diff = None
            for sitemap_datetime_format in SITEMAP_DATETIME_FORMATS:
                try:
                    last_mod = datetime.datetime.strptime(url.find("lastmod", namespaces=url.nsmap).text,
                                                          sitemap_datetime_format)
                    day_diff = (datetime.datetime.now() - last_mod).days
                    break
                except ValueError:
                    pass
            if day_diff is None:
                print("date time in sitemap is not valid")
            elif day_diff < max_day_diff:
                all_loc.append(loc)
        else:
            all_loc.append(loc)
    return all_loc


def generate_sitemap_content_locate(output_url_and_lastmod, locales):
    sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml" >"""
    for url_and_lastmod in output_url_and_lastmod:
        url_sitemap_content = ''
        alternative_sitemap = get_alternative_sitemap_url(url_origin=url_and_lastmod.get('url'),
                                                          locales=locales)
        if url_and_lastmod.get('lastmod') is not None and len(url_and_lastmod.get('lastmod')) > 8:
            url_sitemap_content = f"""
  <url>
    <loc>{url_and_lastmod.get('url')}</loc>
    <lastmod>{url_and_lastmod.get('lastmod')}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.6</priority>{alternative_sitemap}
  </url>"""
        else:
            url_sitemap_content = f"""
  <url>
    <loc>{url_and_lastmod.get('url')}</loc>
    <priority>0.5</priority>
    <changefreq>weekly</changefreq>{alternative_sitemap}
  </url>"""
        sitemap_content += url_sitemap_content
    sitemap_content += '\n</urlset>'
    # print(sitemap_content)
    # from bs4 import BeautifulSoup
    # soup = BeautifulSoup(sitemap_content, 'xml')
    # sitemap_content = soup.prettify(formatter='minimal')
    return sitemap_content


def generate_sitemap_content(output_url_and_lastmod):
    sitemap_content = """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml" >"""
    for url_and_lastmod in output_url_and_lastmod:
        url_sitemap_content = ''
        if url_and_lastmod.get('lastmod') is not None and len(url_and_lastmod.get('lastmod')) > 8:
            url_sitemap_content = f"""
  <url>
    <loc>{url_and_lastmod.get('url')}</loc>
    <lastmod>{url_and_lastmod.get('lastmod')}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.6</priority>
  </url>"""
        else:
            url_sitemap_content = f"""
  <url>
    <loc>{url_and_lastmod.get('url')}</loc>
    <priority>0.5</priority>
    <changefreq>weekly</changefreq>
  </url>"""
        sitemap_content += url_sitemap_content
    sitemap_content += '\n</urlset>'

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(sitemap_content, 'xml')
    # sitemap_content = soup.prettify(formatter='minimal')
    return sitemap_content


def generate_sitemap_index(output_url_and_lastmod):
    sitemap_content = """<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">"""
    for url_and_lastmod in output_url_and_lastmod:
        url_sitemap_content = ''
        if url_and_lastmod.get('lastmod') is not None and len(url_and_lastmod.get('lastmod')) > 8:
            url_sitemap_content = f"""
  <sitemap>
    <loc>{url_and_lastmod.get('url')}</loc>
    <lastmod>{url_and_lastmod.get('lastmod')}</lastmod>
  </sitemap>"""
        else:
            url_sitemap_content = f"""
  <sitemap>
    <loc>{url_and_lastmod.get('url')}</loc>
  </sitemap>"""
        sitemap_content += url_sitemap_content
    sitemap_content += '\n</sitemapindex>'
    # print(sitemap_content)
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(sitemap_content, 'xml')
    # sitemap_content = soup.prettify(formatter='minimal')

    return sitemap_content


from urllib.parse import urlparse


def get_alternative_url(url_origin, locales):
    lst_obj_url = []
    lst_obj_url.append({
        'hreflang': 'x-default',
        'url': url_origin
    })
    for locale in locales:
        path = urlparse(url_origin)
        url_locale = f'{path.scheme}://{path.netloc}/{locale.get("code")}{path.path}'
        if locale.get('default'):
            url_locale = url_origin
        lst_obj_url.append({
            'hreflang': locale.get('iso'),
            'url': url_locale
        })
    return lst_obj_url


def get_alternative_sitemap_url(url_origin, locales):
    lst_obj_url = get_alternative_url(url_origin=url_origin, locales=locales)
    output = ''
    for obj_url in lst_obj_url:
        output += f'''
    <xhtml:link
      rel="alternate"
      hreflang="{obj_url.get("hreflang")}"
      href="{obj_url.get("url")}"
    />'''
    return output


def ping_all_network(url):
    try:
        lst_domain_gg = ['com', 'co.in', 'ca', 'co.uk', 'com.au', 'co.jp', 'de', 'com.br', 'ru', 'nl', 'es', 'fr', 'it',
                         'cz', 'gr', 'ie', 'ch', 'pl', 'se', 'co.za', 'pt', 'be', 'co.nz', 'ro', 'com.ar', 'com.ua',
                         'com.mx', 'no', 'com.hk', 'co.id', 'hu', 'at', 'dk', 'cl', 'rs', 'ee', 'sk', 'lt', 'co.th',
                         'fi', 'com.tw', 'com.pe', 'ae', 'com.my', 'com.uy', 'com.eg', 'hr', 'lu', 'bg', 'com.bd',
                         'co.ke', 'com.py', 'dz', 'com.ph', 'com.tr', 'com.sg', 'co.kr', 'com.vn', 'co.ve', 'co.il',
                         'com.co', 'si', 'com.pr', 'je', 'com.pk', 'com.ni', 'com.gt', 'ba', 'ge', 'com.ec', 'com.mt',
                         'lv', 'com.sa', 'by', 'com.na', 'com.do', 'com.ng', 'co.ug', 'kz', 'com.gh', 'mk', 'com.np',
                         'is', 'com.bo', 'com.et', 'co.ma', 'tn', 'as', 'com.sv', 'ms', 'hn', 'com.kh', 'ac', 'cat',
                         'co.cr', 'com.kw', 'com.pa', 'ci', 'ht', 'lk', 'sc', 'la', 'jo', 'com.lb', 'kg', 'com.bh',
                         'com.jm', 'co.zm', 'mu', 'mn', 'li', 'am', 'bs', 'co.mz', 'com.mm', 'az', 'rw', 'com.af', 'sm',
                         'co.zw', 'tt', 'gp', 'com.om', 'com.qa', 'cm', 'me', 'co.bw', 'gg', 'com.cu', 'com.ly', 'gy',
                         'md', 'ps', 'ad', 'com.ag', 'dm', 'com.cy', 'com.bz', 'mg', 'cd', 'fm', 'com.gi', 'dj', 'cg',
                         'mw', 'gm', 'bi', 'co.ls', 'nr', 'gl', 'iq', 'sn', 'com.fj', 'ne', 'al', 'im', 'sr', 'nu',
                         'cf', 'vu', 'ng', 'sh', 'ml', 'ws', 'co.tz', 'bj', 'pn', 'tm', 'to', 'mv', 'com.sl', 'vg',
                         'com.bn', 'ga', 'tk', 'tl', 'co.ck', 'bf', 'com.pg', 'co.uz', 'so', 'st', 'bt', 'td', 'cv',
                         'com.nf', 'ki', 'com.sb', 'co.vi', 'com.vc', 'com.ai', 'com.tj', 'co.ao', 'tg', 'com.tn',
                         'com.iq', 'cn', 'cc']
        # https://www.google.com/webmasters/tools/ping?sitemap=http://shoutingblogger.blogspot.com/sitemap.xml
        for domain in lst_domain_gg:
            response = requests.get(url=f'https://www.google.{domain}/ping?sitemap={url}')
            response = requests.get(url=f'https://www.google.{domain}/webmasters/tools/ping?sitemap={url}')
        # response = requests.get(url=f'http://www.bing.com/ping?sitemap={urllib.parse.quote(url)}')
        # response = requests.get(url=f'http://webmaster.yandex.com/site/map.xml?host={url}')
    except Exception as e:
        log_error(e)


# output_url_and_lastmod = [
#     {'url': 'https://dantri.com.vn', 'lastmod': '2019-05-08T13:27:24+07:00'},
#     {'url': 'https://genk.com.vn', 'lastmod': '2019-05-08T13:27:24+07:00'}
# ]
#
# generate_sitemap_content(output_url_and_lastmod)
if __name__ == '__main__':
    # gzip_content = get_content_by_gz("/home/obito99/Documents/shopee/sg/sitemap/sitemap.items-1.xml.gz")
    # extract_log_url_from_sitemap(content_string=gzip_content)
    # extract_log_url_from_sitemap_by_lxml(content_string=gzip_content)
    all_location = extract_xlink_from_sitemap_by_lxml(content_string="""
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" xmlns:xhtml="http://www.w3.org/1999/xhtml" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd">
  <url>
    <loc>
<![CDATA[https://shopee.vn/leicavietnam]]>    </loc>
    <lastmod>2021-12-10T14:19:56+07:00</lastmod>
    <priority>0.7</priority>
    <changefreq>daily</changefreq>
    <xhtml:link href="ios-app://959841449/shopeevn/home?navRoute=eyJwYXRocyI6IFt7IndlYk5hdiI6IHsidXJsIjogImh0dHBzOi8vc2hvcGVlLnZuL3Nob3AvP3NpZD02MTE4MDY1MzUmc3U9bGVpY2F2aWV0bmFtJl9nc3JjPWh0dHBzJTNBLy9zaG9wZWUudm4vc2hvcC8lM0ZzaWQlM0Q2MTE4MDY1MzUlMjZzdSUzRGxlaWNhdmlldG5hbSJ9fV19" rel="alternate"/>
    <xhtml:link href="android-app://com.shopee.vn/shopeevn/home?navRoute=eyJwYXRocyI6IFt7IndlYk5hdiI6IHsidXJsIjogImh0dHBzOi8vc2hvcGVlLnZuL3Nob3AvP3NpZD02MTE4MDY1MzUmc3U9bGVpY2F2aWV0bmFtJl9nc3JjPWh0dHBzJTNBLy9zaG9wZWUudm4vc2hvcC8lM0ZzaWQlM0Q2MTE4MDY1MzUlMjZzdSUzRGxlaWNhdmlldG5hbSJ9fV19" rel="alternate"/>
  </url>
</urlset>

    """)
    from helper.url_helper import get_params_from_url
    import base64
    import json

    for extracted_url_obj in all_location:
        if extracted_url_obj is not None and extracted_url_obj.get('lastmod') is not None:
            if extracted_url_obj.get('url') is not None:
                url = extracted_url_obj.get("url")
                nav_router = get_params_from_url(url).get("navRoute", [None])[0]
                if nav_router is not None:
                    data = json.loads(base64.b64decode(nav_router))
                    path = data.get("paths", [None])[0]
                    if path is not None:
                        url = path.get("webNav", {}).get("url")
                        shop_id = get_params_from_url(url).get("sid", [None])[0]
                        if shop_id is not None:
                            print(shop_id)

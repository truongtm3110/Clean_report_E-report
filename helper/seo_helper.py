import requests

from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


def ping_all_network(url):
    try:
        logger.info(f'prepare ping 300bot: {url}')
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
            response = requests.get(url=f'http://www.google.{domain}/ping?sitemap={url}')
            response = requests.get(url=f'https://www.google.{domain}/webmasters/tools/ping?sitemap={url}')
        # response = requests.get(url=f'http://www.bing.com/ping?sitemap={urllib.parse.quote(url)}')
        # response = requests.get(url=f'http://webmaster.yandex.com/site/map.xml?host={url}')
    except Exception as e:
        log_error(e)


def ping_sitemap(urls):
    try:
        for url in urls:
            ping_all_network(url)
            # logger.info(f'PING: {url}')
    except Exception as e:
        log_error(e)

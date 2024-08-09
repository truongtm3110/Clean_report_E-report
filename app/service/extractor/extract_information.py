import re

from helper.logger_helper import LoggerSimple
from helper.phone_helper import padding_zero
from helper.string_helper import remove_text_more_infomation, replace_text_without_alphanumeric, \
    remove_text_without_alphanumeric, normalize_str_unicode, remove_emojis

logger = LoggerSimple(name=__name__).logger


def extract_information_from_text(text):
    """
    Extract:
    Phone
    Email
    Website
    Address
    URL facebook, youtube, instagram, tiktok
    """
    if text is None or len(text) == 0:
        return None
    # text_normalize = remove_text_more_infomation(text)
    text_normalize = normalize_str_unicode(text)
    set_phone = set()
    set_email = set()
    set_fb = set()
    set_youtube = set()
    set_instagram = set()
    set_tiktok = set()
    set_shopee = set()
    set_tiki = set()
    set_lazada = set()
    set_domain = set()
    set_url = set()
    for line in text_normalize.split('\n'):
        set_phone.update(list(extract_phone(line)))
        set_email.update(list(extract_email(line)))
        set_fb.update(list(extract_fb(line)))
        set_youtube.update(list(extract_youtube(line)))
        set_instagram.update(list(extract_instagram(line)))
        set_tiktok.update(list(extract_tiktok(line)))
        set_shopee.update(list(extract_shopee(line)))
        set_tiki.update(list(extract_tiki(line)))
        set_lazada.update(list(extract_lazada(line)))
        set_domain.update(list(extract_domain(line)))
        set_url.update(list(extract_url(line)))

    result = {
        'lst_phone': list(set_phone),
        'lst_email': list(set_email),
        'lst_fb': list(set_fb),
        'lst_youtube': list(set_youtube),
        'lst_instagram': list(set_instagram),
        'lst_tiktok': list(set_tiktok),
        'lst_shopee': list(set_shopee),
        'lst_tiki': list(set_tiki),
        'lst_lazada': list(set_lazada),
        'lst_domain': list(set_domain),
        'lst_url': list(set_url)
    }
    is_valid = False
    for key in result:
        if result[key] is not None and len(result[key]) > 0:
            is_valid = True
    if not is_valid:
        return None
    #     logger.info(is_valid)
    return result


def extract_phone(text, is_padding_zero=False):
    if text is None or len(text) == 0:
        return []
    # text_normalize = remove_text_more_infomation(text)
    text_normalize = text
    # text_normalize = remove_text_without_alphanumeric(str=text_normalize, replacement='', more_character_include='+')
    text_normalize = text_normalize.replace('O', '0').replace('+84', '0') \
        .replace('0️⃣', '0').replace('1️⃣', '1').replace('2️⃣', '2').replace('3️⃣', '3').replace('4️⃣', '4').replace(
        '5️⃣', '5').replace('6️⃣', '6').replace('7️⃣', '7').replace('8️⃣', '8').replace('9️⃣', '9') \
        .replace('.', '').strip()
    text_normalize = remove_emojis(text_normalize)
    if is_padding_zero:
        text_normalize = padding_zero(text_normalize)
    set_phone = set()

    # match = re.findall(r"\b((\()?(0|(\+)?84)((\d+)?\)?)( |\.)?(\d[\.\- ]?){8,10})\b", text_normalize)
    match = re.findall(r"((\()?(0|(\+)?84)((\d+)?\)?)( |\.)?(\d[\.\- ]?){8,10})", text_normalize)
    if match:
        for group in match:
            phone = group[0]
            phone = ''.join(i for i in phone if i.isdigit())
            set_phone.add(phone)
    # số tổng đài
    match_hotline = re.findall(r"\b((\()?(18|19|(\+)?84)((\d+)?\)?)( |\.)?(\d[\.\- ]?){6,8})\b", text_normalize)
    if match_hotline:
        for group in match_hotline:
            phone = group[0]
            # phone = re.sub(r"\(|\)|\s+|\.", "", phone)
            phone = ''.join(i for i in phone if i.isdigit())
            set_phone.add(phone)
    # print(f'{text_normalize} => {lst_phone}')
    # logger.info(f'{text_normalize}')
    return list(set_phone)


def extract_email(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,})\b", line)
    email_set = set()
    if match:
        for email in match:
            email_set.add(email.lower())

    return list(email_set)


def extract_fb(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((facebook\.com|fb\.com|m\.me)\/([^\s])*)", line)
    fb_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                fb_set.add(f'https://' + find[0])
    return list(fb_set)


def extract_domain(text, ignore_domain_popular=True):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9][a-z0-9-]{0,61}[a-z0-9])", line)
    domain_set = set()

    if match:
        for find in match:
            if len(find) > 0:
                domain = find.lower()
                phone = ''.join(i for i in domain.split('.')[-1] if not i.isdigit()).strip()
                if len(phone) == 0:
                    continue
                if ignore_domain_popular:
                    match_domain_ignore = re.match(
                        f"(tiktok\.com|youtube\.com|youtu\.be|facebook\.com|fb\.com|m\.me|instagram\.com|shopee\.vn|tiki\.vn|lazada\.vn|gmail\.com|outlook\.com)",
                        domain)
                    if match_domain_ignore is None:
                        domain_set.add(domain)
                else:
                    domain_set.add(domain)
    return list(domain_set)


def extract_url(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((http|https):\/\/([\w_-]+(?:(?:\.[\w_-]+)+))([\w.,@?^=%&:\/~+#-]*[\w@?^=%&\/~+#-]))", line)
    url_set = set()

    if match:
        for find in match:
            if len(find) > 0:
                url_set.add(find[0])
    return list(url_set)


def extract_youtube(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((youtube\.com|youtu\.be)\/([^\s])*)", line)
    fb_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                fb_set.add(f'https://' + find[0])
    return list(fb_set)


def extract_tiktok(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((\w+.)?(tiktok\.com)\/([^\s])*)", line)
    fb_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                fb_set.add(f'https://' + find[0])
    return list(fb_set)


def extract_instagram(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((instagram\.com)\/([^\s])*)", line)
    url_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                url_set.add(f'https://' + find[0])
    return list(url_set)


def extract_shopee(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((shopee\.vn)\/([^\s])*)", line)
    url_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                url_set.add(f'https://' + find[0])
    return list(url_set)


def extract_tiki(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((tiki\.vn)\/([^\s])*)", line)
    url_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                url_set.add(f'https://' + find[0])
    return list(url_set)


def extract_lazada(text):
    if text is None or len(text) == 0:
        return []
    line = text
    match = re.findall(r"((lazada\.vn)\/([^\s])*)", line)
    url_set = set()
    if match:
        for find in match:
            if len(find) > 0:
                url_set.add(f'https://' + find[0])
    return list(url_set)


if __name__ == '__main__':
    text = """HOTLINE 0589 30 6666 =>     HOTLINE 0589 30 6666
0123456789 => 0123456789
84339702570 => +84339702570
(+84)339702570 => +84339702570
0339702578 => 0339702578
0234567897 => 0234567897
Điện thoại: (028) 38 753 443 => Điện thoại 028 38 753 443
Điện thoại: 028 38125960 => Điện thoại 028 38125960
02838125960 => 02838125960
0589 30 6666 => 0589 30 6666
SDT là 0976 200.663 hoặc +84.33.970.2570 hoặc 033 -970 -2570
    """
    text = """ Dear You!
P.t.k order chuyên bán hàng order quảng châu, order Taobao,1688,Tmall,Pindoudou.
Hàng order 1-2 tuần nên bạn vui lòng cân nhắc khi đặt và KHÔNG HỦY giúp shop ạ!
CALL: 0823109555-0833923666
Hotline: 1800 9025 ( Miễn cước gọi) 🔹 FB:https://www.facebook.com/Sunnyvali.vn/  🔹 Web: https://sunnyvali.vn/ 🔹 Showroom 1: 383 Nguyễn Văn Cừ, Long Biên, Hà Nội 🔹 Showroom 2: 170 Nguyễn Hoàng, TP Đà Nẵng 🔹 Showroom 3: A52 đường D1, phường Tân Thới Nhất, Q12,HCM
1900.6189
"""
    text = """ 
 0788..810..236 từ 9:00-17:00 llo 0788  810..239
Za.IIO: O 9 O 9. 678. 22O giùm shop để đc xử lý nha!
zalo : 0362718537điện báo khách shope là e biết nha em cảm ơn các chị nhiều
0️⃣9️⃣8️⃣2️⃣4️⃣8️⃣0️⃣9️⃣0️⃣6️⃣
0️⃣9️⃣1️⃣1️⃣📞9️⃣6️⃣5️⃣📞2️⃣6️⃣6️⃣ 📞ZALO & FB : 0️⃣9️⃣8️⃣3️⃣📞5️⃣9️⃣4️⃣📞8️⃣7️⃣2️⃣  Địa Chỉ : 68 NGÕ 95 NAM DƯ-HOÀNG MAI-HÀ NỘI
    """
    text = """
    0961.786.786', '0833.442.442', '0328.555.444', 'www.facebook.com', 'zalo.me'
    
    """
    # text = "09 08.39.90.39, 0901.22.07.79"
    # text = "0379046879 - 0937913168"
    # text = "931182228; 0935225768"
    text = "0818851999 - 0984149595"

    # result = extract_phone(text=text)
    # result = extract_email(text=text)
    # result = extract_fb(text=text)
    # result = extract_instagram(text=text)
    # result = extract_tiktok(text=text)
    # result = extract_shopee(text=text)
    # result = extract_domain(text=text)
    # result = extract_url(text=text)
    result = extract_information_from_text(text=text)
    logger.info(result)

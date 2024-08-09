import math
import re
import unicodedata
from collections import Counter
from itertools import chain, repeat

from unidecode import unidecode


def string_to_price_format(str):
    if str is None:
        return str
    return '{:0,.0f}'.format(float(str))


def string_to_number_float(string):
    number_have_comma = re.sub(r'[^\d.,]+', '', string)
    number_float = re.sub(r'[^\d.]+', '', number_have_comma)
    return float(number_float)


def no_accent_vietnamese(str):
    return unidecode(str)


def replace_text_without_alphanumeric(str, replacement='-'):
    str_new = re.sub(
        '[^0-9a-zA-ZAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐE'
        'ÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊO'
        'ÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰY'
        'ÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐE'
        'ÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịo'
        'ôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựy'
        'ýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđe'
        'êéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịo'
        'ôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựy'
        'ýỳỷỹỵ]+',
        replacement, str)
    return str_new


def remove_text_without_alphanumeric(str, more_character_include='.,', replacement=''):
    regex_new = f'^0-9a-zA-ZAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴA' \
                f'ĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐE' \
                f'ÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊO' \
                f'ÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢU' \
                f'ƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴa' \
                f'ăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđe' \
                f'êéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịo' \
                f'ôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợu' \
                f'ưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵa' \
                f'ăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵ{more_character_include} '
    str_new = re.sub(
        rf'[{regex_new}\s]+',
        replacement, str)
    return str_new


def remove_text_without_alpha(str, more_character_include='.,', replace_to=''):
    regex_new = f'^a-zA-ZAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴA' \
                f'ĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐE' \
                f'ÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊO' \
                f'ÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢU' \
                f'ƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴa' \
                f'ăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđe' \
                f'êéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịo' \
                f'ôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợu' \
                f'ưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵa' \
                f'ăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵ{more_character_include} '
    str_new = re.sub(
        rf'[{regex_new}\s]+',
        replace_to, str)
    return str_new


def remove_text_more_infomation(str):
    if str is None:
        return str
    regex = r'(\[[0-9a-zA-ZAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴA' \
            'ĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆI' \
            'ÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢU' \
            'ƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴA' \
            'ĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệi' \
            'íìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợu' \
            'ưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵa' \
            'ăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệi' \
            'íìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợu' \
            r'ưúứùừủửũữụựyýỳỷỹỵ \-&%\/]*\])|(\([' \
            '0-9a-zA-ZAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐE' \
            'ÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊO' \
            'ÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰY' \
            'ÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐEÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴAĂÂÁẮẤÀẰẦẢẲẨÃẴẪẠẶẬĐE' \
            'ÊÉẾÈỀẺỂẼỄẸỆIÍÌỈĨỊOÔƠÓỐỚÒỒỜỎỔỞÕỖỠỌỘỢUƯÚỨÙỪỦỬŨỮỤỰYÝỲỶỸỴaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịo' \
            'ôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựy' \
            'ýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđe' \
            'êéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịo' \
            'ôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựyýỳỷỹỵaăâáắấàằầảẳẩãẵẫạặậđeêéếèềẻểẽễẹệiíìỉĩịoôơóốớòồờỏổởõỗỡọộợuưúứùừủửũữụựy' \
            r'ýỳỷỹỵ \-&%\/]*\))'
    # str_new = normalize_str_unicode(str)
    # str_new = re.sub(regex, '', str_new)
    str_new = re.sub(regex, '', str)
    str_new = remove_text_without_alphanumeric(str_new, more_character_include=r"\s&.\'\"\/")
    str_new = re.sub(' +', ' ', str_new)
    str_new = str_new.strip()
    return str_new


import re


def contains_vietnamese_diacritics(text):
    # Định nghĩa các ký tự có dấu tiếng Việt
    vietnamese_diacritics = re.compile(
        r'[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡ'
        r'ùúụủũưừứựửữỳýỵỷỹđÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴÈÉẸẺẼÊỀẾỆỂỄÌÍỊỈĨ'
        r'ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠÙÚỤỦŨƯỪỨỰỬỮỲÝỴỶỸĐ]'
    )

    # Kiểm tra xem text có chứa ký tự có dấu tiếng Việt không
    return bool(vietnamese_diacritics.search(text))


def strike(text):
    return ''.join(chain.from_iterable(zip(text, repeat('\u0336'))))


def remove_by_terms(str, terms_ignore):
    if str is None:
        return str
    lst_regex = []
    for term in terms_ignore:
        lst_regex.append(f'({term})')
    regex = '|'.join(lst_regex)
    str_new = re.sub(regex, '', str, flags=re.I).strip()
    return str_new


def remove_stopwords(str, stop_word=['ml', 'g', 'gam', 'lit', 'inch']):
    if str is None:
        return str
    tokens = str.split(' ')
    tokens_filtered = [token for token in tokens if token not in stop_word]
    return ' '.join(tokens_filtered)


def slugify(raw, default=None):
    if raw is None:
        return default
    raw = raw if raw.startswith('/') else '/' + raw
    signature = ''

    raw = raw.lower()
    common = 0

    value = unidecode(raw)
    # value = (unicodedata
    #          .normalize('NFKC', value)
    #          .encode('ascii', 'ignore')
    #          .decode('ascii'))
    value = (unicodedata.normalize('NFKC', value))
    value = re.sub(r'[^\w\s-]', '', value).strip()
    value = re.sub(r'[-\s]+', '-', value)
    return value


def calculate_text_similarity(text1, text2):
    WORD = re.compile(r'\w+')

    def get_cosine(vec1, vec2):
        intersection = set(vec1.keys()) & set(vec2.keys())
        numerator = sum([vec1[x] * vec2[x] for x in intersection])

        sum1 = sum([vec1[x] ** 2 for x in vec1.keys()])
        sum2 = sum([vec2[x] ** 2 for x in vec2.keys()])
        denominator = math.sqrt(sum1) * math.sqrt(sum2)

        if not denominator:
            return 0.0
        else:
            return float(numerator) / denominator

    def text_to_vector(text):
        words = WORD.findall(text)
        return Counter(words)

    vector1 = text_to_vector(text1.lower().strip())
    vector2 = text_to_vector(text2.lower().strip())

    cosine = get_cosine(vector1, vector2)

    return cosine


# Ki tu dau tien cua cau viet hoa
# khong ton tai 2 khoang trang lien tiep
# cac ki tu sau khong viet hoa
def normalize_to_normal_sentence_text(sentence):
    if sentence is not None and sentence != '':
        sentence = re.sub(' +', ' ', sentence)
        sentence = sentence.strip()
        sentence = sentence.capitalize()
        return sentence
    return None


def slugify_v2(raw: str, max_length=100):
    raw = raw.lower()
    value = unidecode(raw)
    value = (unicodedata.normalize('NFD', value))
    value = re.sub(r'[^\w\s-]', '', value).strip()
    value = re.sub(r'[-\s]+', '-', value)
    if len(value) > max_length:
        return value[:max_length]
    return value


def normalize_str_unicode(raw):
    if raw is not None and len(raw) > 0:
        return unicodedata.normalize('NFKC', raw)
    return raw

def remove_extra_spaces(text):
    # Sử dụng regex để thay thế nhiều khoảng trắng bằng một khoảng trắng
    return re.sub(r'\s+', ' ', text).strip()



def remove_emojis(data):
    emoj = re.compile("["
                      u"\U0001F600-\U0001F64F"  # emoticons
                      u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                      u"\U0001F680-\U0001F6FF"  # transport & map symbols
                      u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                      u"\U00002500-\U00002BEF"  # chinese char
                      u"\U00002702-\U000027B0"
                      u"\U00002702-\U000027B0"
                      u"\U000024C2-\U0001F251"
                      u"\U0001f926-\U0001f937"
                      u"\U00010000-\U0010ffff"
                      u"\u2640-\u2642"
                      u"\u2600-\u2B55"
                      u"\u200d"
                      u"\u23cf"
                      u"\u23e9"
                      u"\u231a"
                      u"\ufe0f"  # dingbats
                      u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)


def remove_breakline_and_white_space(str):
    if str is None:
        return str
    pattern = re.compile(r'\s\s+')
    return re.sub(pattern, ' ', str.strip())


if __name__ == '__main__':
    print(contains_vietnamese_diacritics('OCEAN IQ Trần Quốc Hoàn'))
    # print(no_accent_vietnamese('lập trình tiếng Việt hihi'))
    # print(replace_text_without_alphanumeric('l1231245.24123,32132ập trình tiếng Việt hihi@@ # [giam giá]@'))
    # print(strike('120.000'))
    # print("\e[3;4;33mthis is a test\n\e[0m")

    # text1 = 'Bao Da Cho Máy Đọc Sách Kindle Paperwhite Gen 4 2019 - VNF-0016'
    # text1 = 'l1231245.24123,32132ập trình tiếng Việt hihi@@ # [giam giá]@'
    # text2 = 'Bao da in hình đẹp mắt cho máy đọc sách Amazon Kindle paperwhite 1 2 3 958 899 - 2312990729'
    # text2 = 'Bao da máy đọc sách Kindle Paperwhite gen 4 -10th - BdKPPW4'
    # text2 = 'Bao da Kindle Paperwhite thế hệ thứ bốn - Bd2'
    # text1 = 'Nồi cơm điện tử Tefal 1.8 lít RK733168'
    text1 = 'Nồi Cơm Điện Sharp KS-COM183MV-WH 1.8L'
    # text2 = 'Ốp Điện thoại di động Iphone 11'
    # text2 = 'Sách giáo khoa di chơi'
    text2 = 'Nồi cơm điện tử Sharp 1.8 lít KS-COM183MV-WH'
    print(calculate_text_similarity(text1, text2))
    # print(text1)
    # print(slugify(text1))
    a = '[TẶNG MASK] Sữa tắm và gội nam 2 trong 1 Paris - Bath & Body Works (295ml)'
    # print('###' + normalize_to_normal_sentence_text(' phUong   PhUnG    ') + '%%%')
    # print(slugify_v2(
    #     '{TẶNG MASK} Sữa tắm và gội nam 2 trong 1 Paris - Bath & Body Works (295ml) chuan chi nhat toi tung thay luon day casc ban oi'))
    raw = """sỉ lẻ
kìm nhặt
dầu gội xả gừng weilaiya
tuýt kem
keo 3m 2 mặt
tẩy vim
sữa đậu
cây lược
đơn hàng
cây mai thái
dây thun cao su
chăn 5kg
kệ xoay 360
ốp lưng s21 ultra
quai vải
hoa tai kiểu
bạt che ô
củ sạc 5v 1a
den led xe
que test pravo
tem chữ dán xe
hình dán jack
unisex áo khoác nỉ
khoá tròn
yếm dây
nồi cháo
quần lot nữ bigsize
trẻ con
dây handmade
kẹo giam cân
thảm loang
quần ống rộng hoạ tiết
vòng tay may
kim bơm
còi xe ô tô
đồng hồ nam đẹp
máy đánh trứng
đùm x1r
chén nấu
ốp lưng ip12
loa kéo jbz 109
nẹp cổ
mận dẻo
akko monet
mai vàng
serum trị thâm mụn
lip sleeping
sọt jean
year
gia de
air jordan 1 retro high
lá sủi cảo
váy chu
sạc 60w
dầu duong tóc
10 áo
trà gói
combo 3 quần lót
nạo rau củ đa năng
áo phông nam đẹp
áo dù nữ
máy ngang câu
nước hoa edp
tân tân
súng bắn ná
khoác hoodie nỉ
bàn chải silicon
vải tơ óng
txd 8s
tất hình
mền đũi
kem chuối
pô xe mio
nón công tử
mũi khoan cnc
cáp ip
jean ống suông rộng
eggy
túi xách nữ elly
cặp kì lân
võ maxxis
giày y350
nhíp nối
bài kaiba
son v16
ha plus whitening serum
bài ma thuật
sandal ab
túi đeo gucci nam
kẹo bò
thẻ học tiếng
ô tô kia morning
hoa hồng thân
6 tuổi
loptop giá rẻ
đầm dư
18 vị la hán
vải sợi tre
đèn pin nextool"""
    # raw_normalized = normalize_str_unicode(raw=raw)
    # print(raw_normalized)
    # print(raw == raw_normalized)
    raw = """Complete Version of remove Emojis✍ 🌷 📌 👈🏻 🖥"""
    # print(remove_emojis(raw))

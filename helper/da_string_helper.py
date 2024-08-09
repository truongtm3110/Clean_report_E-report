from helper.string_helper import normalize_str_unicode, contains_vietnamese_diacritics
from unidecode import unidecode
from langdetect import detect


def is_match_any(text, lst_keyword):
    lst_keyword_uniq = list(set(lst_keyword))
    if len(text) > 0:
        text_unidecode = unidecode(normalize_str_unicode(text).lower())
        for keyword in lst_keyword_uniq:
            kw_unidecode = unidecode(normalize_str_unicode(keyword)).lower()
            if kw_unidecode in text_unidecode:
                return True
    return False


def is_match_at_least_keywords(text, lst_keyword):
    if len(text) > 0:
        lst_keyword_uniq = list(set(lst_keyword))
        text_unidecode = unidecode(normalize_str_unicode(text).lower())
        for keyword in lst_keyword_uniq:
            kw_unidecode = unidecode(normalize_str_unicode(keyword)).lower()
            if kw_unidecode in text_unidecode:
                return True
    return False


from collections import Counter
from typing import List, Tuple


def generate_ngrams(text: str, n: int) -> List[str]:
    words = text.split()
    ngrams = zip(*[words[i:] for i in range(n)])
    return [' '.join(ngram) for ngram in ngrams]


def ngram_frequency(lst_str: List[str], ngram: int) -> List[Tuple[str, int]]:
    all_ngrams = []
    for text in lst_str:
        all_ngrams.extend(generate_ngrams(text, ngram))

    ngram_counts = Counter(all_ngrams)
    sorted_ngram_counts = ngram_counts.most_common()

    return sorted_ngram_counts

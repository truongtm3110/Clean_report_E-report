import re


def select_one_text(element, default=None):
    if element is not None:
        return element.get_text().strip()
    else:
        return default


def select_one_attribute(element, attribute, default=None):
    if element is not None:
        if type(element.get(attribute)) == str:
            return element.get(attribute).strip()
        if type(element.get(attribute)) == list:
            return element.get(attribute)
    else:
        return default


if __name__ == '__main__':
    a = """hihi 1   23
    ha
    123  123
       2222
    """
    # pattern = re.compile(r'\s+')
    pattern = re.compile(r'\s(?=\s)')
    output = re.sub(pattern, '', a.strip())
    print(output)

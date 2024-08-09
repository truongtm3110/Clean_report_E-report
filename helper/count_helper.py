from collections import Counter


def get_occurrence_percentage(array):
    if array is None or len(array) == 0:
        return None
    occurrence = Counter(array)
    occurrence_percentage = [(i, occurrence[i] / len(array)) for i in occurrence]
    return occurrence_percentage


def group_by_key_value(array):
    """
    [
        {
            "name": "tác giả",
            "value": "ABC"
        },
        {
            "name": "nhà xuất bản",
            "value": "def"
        },
    ]
    """
    for item in array:
        pass

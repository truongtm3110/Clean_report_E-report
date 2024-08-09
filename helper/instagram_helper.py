_alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'


def calculate_shortcode_from_post_id(post_id):
    shortcode = ''
    split_index = int(post_id.find('_'))
    if split_index == -1:
        post_id = int(post_id)
    else:
        post_id = int(post_id[:split_index])

    while post_id > 0 and post_id != 0 and len(shortcode) < 20:
        remainder = post_id % 64
        post_id = post_id // 64
        shortcode = _alphabet[remainder] + shortcode
    return shortcode


def test(post_id):
    demo_a = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    shortcode = ''
    split_index = int(post_id.find('_'))
    if split_index == -1:
        post_id = int(post_id)
    else:
        post_id = int(post_id[:split_index])

    while post_id > 0 and post_id != 0 and len(shortcode) < 20:
        remainder = post_id % len(demo_a)
        post_id = post_id // len(demo_a)
        shortcode = demo_a[remainder] + shortcode
    return shortcode


if __name__ == '__main__':
    print(test('1526791424'))
    # 5EEEA981

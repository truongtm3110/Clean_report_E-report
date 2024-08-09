lst_platform = [
    {
        'id': 1,
        'name': 'Shopee'
    },
    {
        'id': 2,
        'name': 'Lazada'
    },
    {
        'id': 3,
        'name': 'Tiki'
    },
    {
        'id': 4,
        'name': 'Sendo'
    },
]


def get_platform_by_id(platform_id):
    for platform in lst_platform:
        if platform.get('id') == int(platform_id):
            return platform
    return None


def get_url_shop(shop_base_id: str, shop_username: str = None):
    url_shop = None
    if shop_base_id is not None:
        platform_id = shop_base_id.split('__')[0]
        shop_id = shop_base_id.split('__')[1]
        if platform_id == '1':
            if shop_username:
                url_shop = f'https://shopee.vn/{shop_username}'
            else:
                url_shop = f'https://shopee.vn/shop/{shop_id}'
            return url_shop

        if platform_id == '2':
            if shop_username:
                url_shop = f'https://www.lazada.vn/shop/{shop_username}'
            return url_shop

        if platform_id == '3':
            if shop_username:
                url_shop = f'https://tiki.vn/cua-hang/{shop_username}'
            return url_shop

    return None
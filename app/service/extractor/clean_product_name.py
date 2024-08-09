from helper.string_helper import remove_by_terms, remove_text_more_infomation, remove_breakline_and_white_space


def clean_product_name(product_name: str):
    if product_name is None or len(product_name) == 0:
        return product_name
    terms_ignore = ['Ảnh Thật', 'Hàng Chính Hãng', 'Shop Yêu Thích', 'Chính Hãng', 'Cam Kết', 'uy tín', 'cam kết',
                    'Yêu thích',
                    'chất lượng cao', 'hàng Auth', 'Auth', 'Có quà', 'Mẫu mới', 'hàng thật', 'giá rẻ', 'giá sỉ',
                    'rẻ vô địch', 'Siêu Giảm giá', 'có giảm giá', 'giảm giá', 'hàng chuẩn', 'giá hủy diệt',
                    'có sẵn',
                    'Màu Ngẫu Nhiên', 'Giao Ngẫu Nhiên', 'Mẫu mới', 'hàng mới về',
                    'FREESHIP', 'có kèm quà tặng', 'kèm quà tặng', 'cao cấp', 'có bill', 'hàng công ty',
                    'xuất xứ mỹ', 'xuất xứ hàn quốc', 'xuất xứ nhật bản', 'xuất xứ nhật',
                    'hot', 'new', 'bất kỳ',
                    'hiệu quả', 'chất lượng',
                    'thị trường',
                    'nhập khẩu',
                    'chính hãng', 'ĐỘC QUYỀN', 'Độc quyền']
    return remove_breakline_and_white_space(
        remove_by_terms(str=remove_text_more_infomation(product_name), terms_ignore=terms_ignore).strip())


if __name__ == '__main__':
    # product_name = '[infinij]5D DIY Full Drill Special Shaped Diamond Painting Pink Horse Cross Stitch'
    # product_name = 'ஐ Kitchen Cherry Pitter Easy Fruit Core Seed Remover Cherry Tools Fruit Corer Kitchen Gadgets Accessories 【zeer】'
    # product_name = '[ CHÍNH HÃNG ] Thẻ nhớ MicroSD Class 10 Tốc độ cao (Đen) 2GB/4GB/8GB/16GB/32GB/64GB, hàng chính hãng chất lượng cao'
    product_name = '[Mã ELMSSEP giảm 6% đơn 50K] SẠC DỰ PHÒNG TÍCH HỢP MÀN HÌNH LED PIN DỰ PHÒNG DUNG LƯỢNG 10000MAH RM WK-161'
    print(clean_product_name(product_name))

import pandas as pd
import numpy as np


def match_kw_in_name(name, lst):
    name = name.lower().strip()
    for kw in lst:
        kw = kw.lower().strip()
        kw_split_num = len(kw.split())
        words = name.lower().strip().split()
        ngrams = []
        for i in range(kw_split_num, 0, -1):
            ngrams.extend([' '.join(ngram) for ngram in zip(*[words[j:] for j in range(i)])])
            return kw
    return None


def match_kw_in_last_key_name(name, lst):
    name = name.lower().strip().split()
    for kw in lst:
        kw = kw.lower().strip()
        kw_split_num = len(kw.split())
        name_check = " ".join(name[-kw_split_num:])
        if kw in name_check:
            return kw
    return None

# def match_kw_in_name(name, lst):
#     name = name.lower().strip()
#     for kw in lst:
#         kw = kw.lower().strip()
#         if kw in name.split(" "):
#             return kw
#     return None
#
#
# def match_kw_in_last_key_name(name, lst):
#     name = name.lower().strip()
#     for kw in lst:
#         kw = kw.lower().strip()
#         if name.endswith(kw):
#             return kw
#     return None


def cus_name(row):
    # Kiểm tra và thay thế giá trị nếu không null
    if pd.notnull(row["people"]):
        row["name"] = row["name"].replace(row["people"], "").replace("  ", " ")
    if pd.notnull(row["remove"]):
        row["name"] = row["name"].replace(row["remove"], "").replace("  ", " ")
    if pd.notnull(row["remove_by_last_word"]):
        row["name"] = row["name"].replace(row["remove_by_last_word"], "").replace("  ", " ")
    if pd.notnull(row["lst_consider"]):
        row["name"] = row["name"].replace(row["lst_consider"], "").replace("  ", " ")
    if pd.notnull(row["lst_origin"]):
        row["name"] = row["name"].replace(row["lst_origin"], "").replace("  ", " ")

    return row["name"]


path_excel = r"C:\Users\khami\Downloads\155k_lay_ereport.xlsx"

df = pd.read_excel(path_excel)
df = df[df["remove_edited"] != "x"]

lst_people = ["cho nữ", "cho nam", "cho bé", "cho trẻ", "cho bà", "cho mẹ", "nam", "nữ", "unisex", "em bé", "trẻ em",
              "em trẻ",
              ]
lst_people = sorted(lst_people, key=lambda x: len(x.split()), reverse=False)

lst_remove = [
    "độc lạ", "đế mềm", "giá sỉ", "chính hàng", "abc", "nhập khẩu", "lịch lãm", "chống nước", "ulzzang", "ulzang",
    "bigsize", "big size", "dạo phố", "đeo nhà", "đi chơi", "đi chùa", "đi nắng", "mang trong", "tiện lợi",
    "ullzang", "2022", "2023", "2024", "đi tiệc", "fake", "50k", "100k", "200k", "300k", "99k", "điệu", "xả kho",
    " hoa đá", "cô dâu", "ở nhà", "chống trượt", "khách sạn", "bling", "fear of god", "sinh nhật", "đi mưa",
    "độc và lạ", "thú ý", "tĩnh điện",
    "êm chân", "đơn giản", "nằm giường", "nằm võng", "trong phòng", "tự làm", "tư thế", "background", "béc", "bì",
    "văn phòng", "cần điện", "sân khấu", "vũ trường", "phòng bay", "phòng khách", "phòng ngủ", "linh kiện", "dụng cụ",
    "sang", "vương miện", "360", "rửa tay", "rửa chén", "hiển thị", "tập gym", "bán hàng", "thủ công", "theo nhạc",
    "chính xác", "thắng lợi", "sân vườn", "yêu cầu", "văn phòng", "thời gian", "phòng cưới", "dày dặn", "may mắn",
    "bình an", "trừ sâu", "nhà tắm", "cực mạnh", "cực nhanh", "phục vụ", "tài lộc", "tự làm", "trọn bộ", "làm việc",
    "giường ngủ", "gọn nhẹ", "nhỏ gọn", "bé nhỏ", "sinh viên", "bụng kinh", "thương hiệu", "chườm bụng",
    "chườm nóng", "cắm điện", "ban công", "kỹ thuật số", "chuyên dụng", "tổng hợp", "khuyễn mãi", "khuyến mại",
    "xi lanh", "gioăng", "cảnh báo", "văn phòng", "handmade", "cosplay", "thu nhỏ", "2hand", "second hand", "hip hop",
    "hiphop", "ngầu", "sang chảnh", "thiết kế", "tiktok", "du lịch", "sticker", "tự làm", "bling", "dự tiệc",
    "phá cách", "sang trọng", "phong thủy", "huy hiệu", "cổ trang", "đơn giản", "tag"
]
lst_remove = sorted(lst_remove, key=lambda x: len(x.split()), reverse=False)

lst_remove_by_last_word = ["mảnh", "miếng", "5p", "7p", "10p", "11p", "3p", "9p", "12p", "3p", "phân", "lớp", "gói",
                           "miếng quần", "tờ", "bấc", "bóng", "cánh", "bậc", "ngăn", "hốc", "hộc", "w", "d", "tờ",
                           "cuộn", "lỗ", "v", "cell", "h", "viên", "giả", "đồng", "chất", "lít"

                           ]
lst_remove_by_last_word = sorted(lst_remove_by_last_word, key=lambda x: len(x.split()), reverse=False)

lst_consider = [
    "vui vẻ", "official", "thể thao", "basic", "hoàng gia", "dinh dưỡng", "2in1", "3in1", "món", "đồ"
]
lst_consider = sorted(lst_consider, key=lambda x: len(x.split()), reverse=False)

lst_origin = [
    "hàn quốc", "trung quốc", "thái lan", "hàng quốc", "mỹ", "đức", "hàn", "nhật bản", "đài loan",
]
lst_origin = sorted(lst_origin, key=lambda x: len(x.split()), reverse=False)

# Tạo df để xử lý từ khóa
df_cus = df[["id", "name"]]

# Thêm cột và thêm điều kiện
df_cus.loc[:, "people"] = df_cus["name"].apply(match_kw_in_name, args=(lst_people,))
df_cus.loc[:, "remove"] = df_cus["name"].apply(match_kw_in_name, args=(lst_remove,))
df_cus.loc[:, "remove_by_last_word"] = df_cus["name"].apply(match_kw_in_last_key_name, args=(lst_remove_by_last_word,))
df_cus.loc[:, "lst_consider"] = df_cus["name"].apply(match_kw_in_name, args=(lst_consider,))
df_cus.loc[:, "lst_origin"] = df_cus["name"].apply(match_kw_in_name, args=(lst_origin,))

# Tạo bản sao của df_cus
df_cus = df_cus.copy()

df_cus["name_"] = df_cus.apply(cus_name, axis=1)

df_cus.to_excel(r"C:\Users\khami\Downloads\155k_lay_ereport_done_code.xlsx", index=False)

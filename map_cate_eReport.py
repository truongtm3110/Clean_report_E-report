import pandas as pd
import helper.helper_eReport
from tqdm import tqdm

from collections import Counter

# Đọc dữ liệu từ file Excel
path = r"D:\TUAN\OneDrive - Turry\Metric_T\e-report\data_eReport_map_cate_2907.xlsx"
df = pd.read_excel(path, sheet_name="Sheet1")

# Lọc các hàng không có "x" trong cột "remove_edited"
df_filtered = df[df["remove_edited"] != "x"].copy()

# Thay đổi tên danh mục theo quy định
df_filtered["category_name"] = df_filtered["category_name"].replace({
    "Mô tô, xe máy": "Mô tô, xe máy",
    "Ô tô": "Mô tô, xe máy",
    "Camera giám sát": "Cameras & Flycam",
    "Giày Dép Nữ": "Giày Dép Nam",
    "Thời Trang Nam": "Thời Trang Nữ",
    "Thời trang trẻ em & trẻ sơ sinh": "Thời Trang Nữ",
    "Túi Ví Nam": "Túi Ví Nữ",
    "Chăm sóc tóc": "Chăm sóc cá nhân",
    "Chuột & Bàn Phím": "Máy tính & Laptop",
    "Sức Khỏe": "Sắc Đẹp",
    "Thiết Bị Âm Thanh": "Thiết Bị Điện Gia Dụng",
    "Đồ gia dụng nhà bếp": "Nhà cửa & Đời sống",
    "Thể Thao & Dã Ngoại": "Thời Trang Nữ",
    "Phụ Kiện Thời Trang": "Thời Trang Nữ",
    "Nhà cửa & Đời sống": "Nhà cửa & Đời sống",
    "Mẹ & Bé": "Nhà cửa & Đời sống"
})


df_filtered["cate1"] = df_filtered["name"].apply(lambda x: " ".join(x.split()[:1]))
df_filtered["cate2"] = df_filtered["name"].apply(lambda x: " ".join(x.split()[:2]))
df_filtered["cate3"] = df_filtered["name"].apply(lambda x: " ".join(x.split()[:3]))

df_lv1 = df_filtered.groupby(["category_name", "cate1"]).size().reset_index(name='count')
df_lv2 = df_filtered.groupby(["category_name", "cate2"]).size().reset_index(name='count')
df_lv3 = df_filtered.groupby(["category_name", "cate3"]).size().reset_index(name='count')

def get_filtered_df(df, num):
    return df[df['count'] > num]

lst_output = []
## Phân loại cate
print("Tạo dict phân loại:....")
for cate in tqdm(df_filtered["category_name"].unique()):
    dict_lv1 = get_filtered_df(df_lv1[df_lv1["category_name"] == cate], 80)
    dict_lv2 = get_filtered_df(df_lv2[df_lv2["category_name"] == cate], 40)
    dict_lv3 = get_filtered_df(df_lv3[df_lv3["category_name"] == cate], 3)

    for _, lv1_row in dict_lv1.iterrows():
        lv1_name = lv1_row["cate1"]
        lv1_volume = lv1_row["count"]
        lst_children_lv2 = []

        for _, lv2_row in dict_lv2.iterrows():
            lv2_name = lv2_row["cate2"]
            lv2_volume = lv2_row["count"]
            if lv2_name.startswith(f"{lv1_name} "):
                lst_children_lv3 = []
                lv2_total_volume = lv2_volume

                for _, lv3_row in dict_lv3.iterrows():
                    lv3_name = lv3_row["cate3"]
                    lv3_volume = lv3_row["count"]
                    if lv3_name.startswith(f"{lv2_name} "):
                        if lv3_volume > 0.8 * lv2_volume:
                            # Nếu volume của lv3 lớn hơn 80% volume của lv2, chuyển lv3 lên lv2
                            lst_children_lv2.append({
                                "name_report": lv3_name,
                                "volume_lv2": lv3_volume,
                                "level": 2
                            })
                        else:
                            lst_children_lv3.append({
                                "name_report": lv3_name,
                                "volume_lv3": lv3_volume,
                                "level": 3
                            })

                            lst_children_lv2.append({
                                "name_report": lv2_name,
                                "volume_lv2": lv2_total_volume,
                                "level": 2,
                                "children": lst_children_lv3
                            })

                # if lst_children_lv3 or lv2_total_volume > 0:
                #     lst_children_lv2.append({
                #         "name_report": lv2_name,
                #         "volume_lv2": lv2_total_volume,
                #         "level": 2,
                #         "children": lst_children_lv3
                #     })

        if lst_children_lv2 or lv1_volume>0:
            lst_output.append({
                "name_catel_lv1": cate,
                "name_cate_edited": lv1_name,
                "volume": lv1_volume,
                "level": 1,
                "children": lst_children_lv2
            })
## sort theo thứ tự
lst_output_sorted = sorted(lst_output, key=lambda x: x['name_cate_edited'])
for lv1 in lst_output_sorted:
    if 'children' in lv1:
        lv1['children'] = sorted(lv1['children'], key=lambda x: x['name_report'])

print("Tạo dict cate:.........")
## Tạo dict
dict_cate = {}
for item in tqdm(lst_output_sorted):
    name_cate_edited = item['name_cate_edited']
    name_catel_lv1 = item['name_catel_lv1']
    if name_catel_lv1 not in dict_cate:
        dict_cate[name_catel_lv1] = {}
    if name_cate_edited not in dict_cate[name_catel_lv1]:
        dict_cate[name_catel_lv1][name_cate_edited] = {}

    for child_lv2 in item['children']:
        name_report_lv2 = child_lv2['name_report']
        if name_report_lv2 not in dict_cate[name_catel_lv1][name_cate_edited]:
            dict_cate[name_catel_lv1][name_cate_edited][name_report_lv2] = set()

        for child_lv3 in child_lv2.get('children', []):
            name_report_lv3 = child_lv3['name_report']
            dict_cate[name_catel_lv1][name_cate_edited][name_report_lv2].add(name_report_lv3)

def find_cate(name, dict_cate):
    name_spl = name.split()
    if helper.helper_eReport.check_number_in_word(name_spl[0]):
        name = " ".join(name_spl[1:])

    for cate, lv1_dict in dict_cate.items():
        for lv1_name, lv2_dict in lv1_dict.items():
            for lv2_name, lv3_set in lv2_dict.items():
                for lv3_name in lv3_set:
                    if name.startswith(f"{lv3_name} "):
                        return cate, lv1_name, lv2_name, lv3_name
                if name.startswith(f"{lv2_name} "):
                    return cate, lv1_name, lv2_name, None
            if name.startswith(f"{lv1_name} "):
                return cate, lv1_name, None, None
        if name.startswith(f"{cate} "):
            return cate, None, None, None
    return None, None, None, None

print("Đang tag vô file excel:.........")
for idx, row in df.iterrows():
    name_clean = row['name']
    cate, lv1_name, lv2_name, lv3_name = find_cate(name_clean, dict_cate)
    df.at[idx, 'cate'] = cate
    df.at[idx, 'lv1_name'] = lv1_name
    df.at[idx, 'lv2_name'] = lv2_name
    df.at[idx, 'lv3_name'] = lv3_name


output_path = r"D:\TUAN\OneDrive - Turry\Metric_T\e-report\data_eReport_map_2907_3.xlsx"
helper.helper_eReport.df_to_excel(df,output_path, index=False)

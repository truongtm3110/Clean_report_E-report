import pandas as pd
from collections import Counter
from tqdm import tqdm

path = r"D:\TUAN\OneDrive - Turry\Metric_T\e-report\data_eReport_map_cate_2907.xlsx"
df = pd.read_excel(path, sheet_name="Sheet1")
dict_ = df.to_dict(orient='records')

lst_dict_remake = []
for pro in dict_:
    name = pro["name"]
    cate = pro["category_name"]
    remove = pro["remove_edited"]

    if remove != "x":
        if cate in ["Mô tô, xe máy",  "Ô tô"]:
            cate = "Mô tô, xe máy"
        if cate in ['Camera giám sát']:
            cate = 'Cameras & Flycam'
        elif cate in ["Giày Dép Nữ"]:
            cate = "Giày Dép Nam"
        elif cate in ["Thời Trang Nam", "Thể Thao & Dã Ngoại","Phụ Kiện Thời Trang","Thời trang trẻ em & trẻ sơ sinh", "Mẹ & Bé"]:
            cate = "Thời Trang Nữ"
        elif cate in [ 'Túi Ví Nam']:
            cate =  'Túi Ví Nữ'
        elif cate in [ 'Chăm sóc tóc']:
            cate =  'Chăm sóc cá nhân'
        elif cate in ['Chuột & Bàn Phím']:
            cate =  'Máy tính & Laptop'
        elif cate in ['Sức Khỏe']:
            cate =  'Sắc Đẹp'
        elif cate in ['Thiết Bị Âm Thanh']:
            cate =  'Thiết Bị Điện Gia Dụng'
        elif cate in ['Đồ gia dụng nhà bếp', "Nhà cửa & Đời sống"]:
            cate = "Nhà cửa & Đời sống"

        lst_dict_remake.append({
            "name": name,
            "cate": cate
        })

lst_cate = set(d['cate'] for d in lst_dict_remake)
dict_cate_lv1 = {cate: [] for cate in lst_cate}
dict_cate_lv2 = {cate: [] for cate in lst_cate}
dict_cate_lv3 = {cate: [] for cate in lst_cate}
dict_cate_lv4 = {cate: [] for cate in lst_cate}

for pro in lst_dict_remake:
    name = pro['name']
    cate = pro['cate']

    cat1 = " ".join(name.split(" ")[:1])
    cat2 = " ".join(name.split(" ")[:2])
    cat3 = " ".join(name.split(" ")[:3])
    cat4 = " ".join(name.split(" ")[:4])

    if cate in dict_cate_lv1:
        dict_cate_lv1[cate].append(cat1)
        dict_cate_lv2[cate].append(cat2)
        dict_cate_lv3[cate].append(cat3)
        dict_cate_lv4[cate].append(cat4)

def count_freq(lst):
    return Counter(lst)

cat1 = {cate: count_freq(names) for cate, names in dict_cate_lv1.items()}
cat2 = {cate: count_freq(names) for cate, names in dict_cate_lv2.items()}
cat3 = {cate: count_freq(names) for cate, names in dict_cate_lv3.items()}
cat4 = {cate: count_freq(names) for cate, names in dict_cate_lv4.items()}

def build_directory_tree(cat1, cat2, cat3, cat4):
    hierarchy = []
    for cate, lv1_counter in cat1.items():
        lv1_list = []
        for lv1, count1 in lv1_counter.items():
            if count1 < 50:
                continue
            lv2_list = []
            for lv2, count2 in cat2[cate].items():
                if not lv2.startswith(lv1) or count2 < 20:
                    continue
                lv3_list = []
                for lv3, count3 in cat3[cate].items():
                    if not lv3.startswith(lv2) or count3 < 6:
                        continue
                    lv4_list = []
                    for lv4, count4 in cat4[cate].items():
                        if not lv4.startswith(lv3) or count4 < 3:
                            continue

                        if count4 >= 0.8 * count3:
                            print(f"Thay thế cate {lv4} - {count4} cho {lv3} - {count3}")
                            lv3 = lv4
                            count3 = count4
                        else:
                            lv4_list.append({
                                'name_report': lv4,
                                'volume_lv4': count4,
                                'level': 4
                            })

                    if count3 >= 0.8 * count2:
                        print(f"Thay thế cate {lv3} - {count3} cho {lv2} - {count2}")
                        lv2 = lv3
                        count2 = count3
                        lv3_list = lv4_list
                    else:
                        lv3_list.append({
                            'name_report': lv3,
                            'volume_lv3': count3,
                            'level': 3,
                            'children': lv4_list
                        })

                if count2 >= 0.8 * count1:
                    print(f"Thay thế cate {lv2} - {count2} cho {lv1} - {count1}")
                    lv1 = lv2
                    count1 = count2
                    lv2_list = lv3_list
                else:
                    lv2_list.append({
                        'name_report': lv2,
                        'volume_lv2': count2,
                        'level': 2,
                        'children': lv3_list
                    })
            lv1_list.append({
                'name_catel_lv1': cate,
                'name_cate_edited': lv1,
                'volume': count1,
                'level': 1,
                'children': lv2_list
            })
        hierarchy.append({
            'category': cate,
            'children': lv1_list
        })
    return hierarchy

hierarchy = build_directory_tree(cat1, cat2, cat3, cat4)

def create_category_dict(hierarchy):
    category_dict = {}

    for cate in hierarchy:
        category = cate['category']
        category_dict[category] = {}

        for lv1 in cate['children']:
            lv1_name = lv1['name_cate_edited']
            category_dict[category][lv1_name] = {}

            for lv2 in lv1.get('children', []):
                lv2_name = lv2['name_report']
                category_dict[category][lv1_name][lv2_name] = {}

                for lv3 in lv2.get('children', []):
                    lv3_name = lv3['name_report']
                    category_dict[category][lv1_name][lv2_name][lv3_name] = {}
    return category_dict

category_dict = create_category_dict(hierarchy)

def tag_data(row, category_dict):
    name = row['name']
    name_spl = name.split()
    if helper.helper_eReport.check_number_in_word(name_spl[0]):
        name = " ".join(name_spl[1:])

    for category, lv1_dict in category_dict.items():
        for lv1_name, lv2_dict in lv1_dict.items():
            for lv2_name, lv3_dict in lv2_dict.items():
                for lv3_name in lv3_dict.keys():
                    if name.startswith(lv3_name):
                        return pd.Series([category, lv1_name, lv2_name, lv3_name])

    for category, lv1_dict in category_dict.items():
        for lv1_name, lv2_dict in lv1_dict.items():
            for lv2_name, lv3_dict in lv2_dict.items():
                if name.startswith(lv2_name):
                    return pd.Series([category, lv1_name, lv2_name, ''])

    for category, lv1_dict in category_dict.items():
        for lv1_name, lv2_dict in lv1_dict.items():
            if name.startswith(lv1_name):
                return pd.Series([category, lv1_name, '', ''])

    return pd.Series(['', '', '', ''])

tqdm.pandas()
df[['cate_new', 'cat1', 'cat2', 'cat3']] = df.progress_apply(lambda row: tag_data(row, category_dict), axis=1)

output_path = r"D:\TUAN\OneDrive - Turry\Metric_T\e-report\data_eReport_map_2907_3.xlsx"
helper.helper_eReport.df_to_excel(df, output_path, index=False)
